use crate::mysql::error::{Error, Result};
use rsa::pkcs8::DecodePublicKey;
use rsa::{Oaep, RsaPublicKey};
use sha1::{Digest, Sha1};
use sha2::Sha256;

/// Auth plugin
///
/// this trait and impls refers to MySQL 5.1.49
/// JDBC implementation but simplified
pub trait AuthPlugin {
    /// name of this plugin
    fn name(&self) -> &str;

    /// set credentials
    ///
    /// this method should be called before next() method
    fn set_credential(&mut self, username: &[u8], password: &[u8]);

    /// process authentication handshake data from server
    /// and optionally produce data to be sent to server
    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;

    /// generate initial auth response.
    #[inline]
    fn gen_init_auth_resp(
        &mut self,
        username: &[u8],
        password: &[u8],
        mut seed: &[u8],
    ) -> Result<Vec<u8>> {
        if let Some(0x00) = seed.last() {
            // remove trailing 0x00 byte
            seed = &seed[..seed.len() - 1];
        }
        let mut output = vec![];
        self.set_credential(username, password);
        self.next(seed, &mut output)?;
        Ok(output)
    }
}

pub enum AuthPluginImpl {
    Native(Native),
    CachingSha2(CachingSha2),
}

impl AuthPluginImpl {
    #[inline]
    pub fn new(plugin_name: &[u8]) -> Result<Self> {
        match plugin_name {
            b"mysql_native_password" => Ok(AuthPluginImpl::Native(Native::default())),
            b"caching_sha2_password" => {
                Ok(AuthPluginImpl::CachingSha2(CachingSha2::with_ssl(false)))
            }
            _ => Err(Error::AuthPluginNotSupported(Box::new(
                String::from_utf8_lossy(plugin_name).to_string(),
            ))),
        }
    }
}

impl AuthPlugin for AuthPluginImpl {
    #[inline]
    fn name(&self) -> &str {
        match self {
            AuthPluginImpl::Native(n) => n.name(),
            AuthPluginImpl::CachingSha2(c) => c.name(),
        }
    }

    #[inline]
    fn set_credential(&mut self, username: &[u8], password: &[u8]) {
        match self {
            AuthPluginImpl::Native(n) => n.set_credential(username, password),
            AuthPluginImpl::CachingSha2(c) => c.set_credential(username, password),
        }
    }

    #[inline]
    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        match self {
            AuthPluginImpl::Native(n) => n.next(input, output),
            AuthPluginImpl::CachingSha2(c) => c.next(input, output),
        }
    }
}

/// implementation of mysql_native_password
#[derive(Debug, Default)]
pub struct Native {
    password: Vec<u8>,
}

impl AuthPlugin for Native {
    #[inline]
    fn name(&self) -> &str {
        "mysql_native_password"
    }

    #[inline]
    fn set_credential(&mut self, _username: &[u8], password: &[u8]) {
        self.password = Vec::from(password);
    }

    #[inline]
    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if self.password.is_empty() {
            // no password
            return Ok(());
        }
        let mut tmp = [0u8; 20];
        scramble411(&self.password, input, &mut tmp)?;
        output.extend(tmp);
        Ok(())
    }
}

// output must be 20 bytes long.
#[inline]
fn scramble411(password: &[u8], seed: &[u8], out: &mut [u8]) -> Result<()> {
    let mut hasher = Sha1::new();
    let stage1 = {
        hasher.update(password);
        hasher.finalize_reset()
    };
    let stage2 = {
        hasher.update(stage1);
        hasher.finalize_reset()
    };
    let seed_hash = {
        hasher.update(seed);
        hasher.update(stage2);
        hasher.finalize()
    };
    for (ob, (b1, b2)) in out.iter_mut().zip(seed_hash.iter().zip(stage1.iter())) {
        *ob = b1 ^ b2;
    }
    Ok(())
}

/// implementation of caching_sha2_password
#[derive(Debug)]
pub struct CachingSha2 {
    password: Vec<u8>,
    seed: Vec<u8>,
    stage: CachingSha2Stage,
    ssl: bool,
}

impl CachingSha2 {
    #[inline]
    pub fn with_ssl(ssl: bool) -> Self {
        CachingSha2 {
            password: vec![],
            seed: vec![],
            stage: CachingSha2Stage::FastAuthSendScramble,
            ssl,
        }
    }
}

impl CachingSha2 {
    // XOR password with given seed.
    // Password includes null-byte suffix.
    #[inline]
    fn xor_password(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.password.len() + 1];
        for (idx, (ob, b)) in buf
            .iter_mut()
            .zip(self.password.iter().chain(std::iter::once(&0)))
            .enumerate()
        {
            *ob = *b ^ self.seed[idx % self.seed.len()];
        }
        buf
    }
}

impl AuthPlugin for CachingSha2 {
    #[inline]
    fn name(&self) -> &str {
        "caching_sha2_password"
    }

    #[inline]
    fn set_credential(&mut self, _username: &[u8], password: &[u8]) {
        self.password = Vec::from(password);
    }

    #[inline]
    fn next(&mut self, mut input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if self.password.is_empty() {
            output.push(0);
            return Ok(());
        }
        match self.stage {
            CachingSha2Stage::FastAuthSendScramble => {
                log::debug!("caching_sha2_password enters fast auth scramble stage.");
                // Save seed. It may also be used in full auth process.
                self.seed.extend_from_slice(input);
                let mut tmp = [0u8; 32];
                scramble_caching_sha2(&self.password, &self.seed, &mut tmp)?;
                output.extend(tmp);
                self.stage = CachingSha2Stage::FastAuthReadResult;
                Ok(())
            }
            CachingSha2Stage::FastAuthReadResult => {
                log::debug!("caching_sha2_password enters fast auth result stage.");
                if input.is_empty() {
                    return Err(Error::MalformedPacket());
                }
                match input[0] {
                    3 => Ok(()), // complete fast auth
                    4 => {
                        // Here we enter full auth process.
                        // This can happen in case server does not cache the authentication information
                        // yet, or the fast auth scramble fails.
                        // Full auth has three options to continue:
                        // 1. Current connection is established with SSL.
                        //    So we can directly send raw password(null-end-str) on the connection.
                        //    Server will verify it then.
                        //    Capability flag: SSL.
                        // 2. Current connection without SSL.
                        //    We need to retrieve pubkey from server, encrypt password and send it.
                        //    connection attribute: 'allowPublicKeyRetrieval'
                        // 3. pubkey file is stored locally that we can use it directly.
                        //    connection attribute: 'serverRSAPublicKeyFile'
                        if self.ssl {
                            // ssl established, just send password in plain text
                            output.extend_from_slice(&self.password);
                            output.push(0);
                            return Ok(());
                        }
                        // todo: option to read pubkey file and send encrypted password
                        // retrieve pubkey
                        output.push(2); // request server pubkey
                        self.stage = CachingSha2Stage::FullAuthPubkeyRequest;
                        Ok(())
                    }
                    _ => Err(Error::MalformedPacket()),
                }
            }
            CachingSha2Stage::FullAuthPubkeyRequest => {
                log::debug!("caching_sha2_password enters full auth stage");
                let input = &mut input;
                let pubkey_str =
                    std::str::from_utf8(input).map_err(|_| Error::InvalidUtf8String())?;
                let pubkey = RsaPublicKey::from_public_key_pem(pubkey_str)
                    .map_err(|_| Error::AuthRSACantParse())?;
                // todo: support old version(less than 8.0.5) of MySQL
                // Use RSA/ECB/PKCS1Padding instead of RSA/ECB/OAEPWithSHA-1AndMGF1Padding.
                let buf = self.xor_password();
                let padding = Oaep::new_with_mgf_hash::<Sha1, Sha1>();
                let mut rng = rand::thread_rng();
                let res = pubkey
                    .encrypt(&mut rng, padding, &buf)
                    .map_err(|_| Error::AuthRSACantEncrypt())?;
                output.extend(res);
                Ok(())
            }
        }
    }
}

/// Scrambling for caching_sha2_password plugin
///
/// Scramble = XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
#[inline]
fn scramble_caching_sha2(password: &[u8], seed: &[u8], out: &mut [u8]) -> Result<()> {
    let mut hasher = Sha256::new();
    // dig1 = SHA2(password)
    let dig1 = {
        hasher.update(password);
        hasher.finalize_reset()
    };
    // dig2 = SHA2(dig1)
    let dig2 = {
        hasher.update(dig1);
        hasher.finalize_reset()
    };
    // scramble1 = SHA2(dig2, seed)
    let scramble1 = {
        hasher.update(dig2);
        hasher.update(seed);
        hasher.finalize_reset()
    };
    // result = XOR(dig1, scramble1)
    for (ob, (b1, b2)) in out.iter_mut().zip(dig1.iter().zip(scramble1.iter())) {
        *ob = b1 ^ b2;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum CachingSha2Stage {
    FastAuthSendScramble,
    FastAuthReadResult,
    // FastAuthComplete,
    // FullAuthSSL,
    // FullAuthPubkeyFile,
    FullAuthPubkeyRequest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_native_password() {
        let seed = vec![
            30, 7, 75, 32, 104, 55, 62, 80, 50, 70, 113, 58, 5, 1, 115, 83, 96, 79, 62, 2, 0,
        ];
        let pass = b"123456";
        let expected = vec![
            120, 250, 12, 222, 161, 31, 159, 240, 110, 225, 255, 230, 123, 243, 220, 213, 199, 205,
            145, 53,
        ];

        let mut plugin = AuthPluginImpl::new(b"mysql_native_password").unwrap();
        let resp = plugin.gen_init_auth_resp(b"", pass, &seed).unwrap();
        assert_eq!(expected, resp);
    }

    #[test]
    fn test_mysql_plugin_name() {
        for name in ["mysql_native_password", "caching_sha2_password"] {
            let plugin = AuthPluginImpl::new(name.as_bytes()).unwrap();
            assert_eq!(name, plugin.name());
        }
    }
}
