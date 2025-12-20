use crate::mysql::MySQLServer;
use doradb_catalog::Catalog;
use doradb_plan::lgc::LgcPlan;
use futures_lite::{AsyncRead, AsyncWrite};
// use doradb_plan::phy::PhyPlan;
use doradb_protocol::buf::ByteBuffer;
use doradb_protocol::mysql::cmd::MyCmd;
use doradb_protocol::mysql::conn::MyConn;
use doradb_protocol::mysql::error::{Error, Result};
use doradb_protocol::mysql::flag::StatusFlags;
use doradb_protocol::mysql::principal::Principal;
use doradb_protocol::mysql::serde::MyDeser;
use doradb_sql::ast::{Query, Statement};
use doradb_sql::parser::dialect::MySQL;
use doradb_sql::parser::parse_stmt;
pub struct Session<'a, C: Catalog, T> {
    server: &'a MySQLServer<C>,
    conn: MyConn<T>,
    status_flags: StatusFlags,
    curr_db: Option<String>,
    principal: Option<Principal>,
}

impl<'a, C: Catalog, T: AsyncRead + AsyncWrite + Unpin> Session<'a, C, T> {
    #[inline]
    pub fn new(server: &'a MySQLServer<C>, conn: MyConn<T>) -> Self {
        Session {
            server,
            conn,
            status_flags: StatusFlags::AUTOCOMMIT,
            curr_db: None,
            principal: None,
        }
    }

    #[inline]
    pub async fn start(mut self) {
        let read_buf = ByteBuffer::with_capacity(4096);
        match self
            .conn
            .server_handshake(&self.server.spec, self.status_flags, &read_buf)
            .await
        {
            Ok((curr_db, principal)) => {
                log::debug!("principal {:?} login suceeded", principal);
                self.principal.replace(principal);
                // todo: check existence of current database
                if let Some(curr_db) = curr_db {
                    self.curr_db.replace(curr_db);
                }
            }
            Err(e) => {
                self.conn.send_err(e).await;
                return;
            }
        }
        self.conn.reset_pkt_nr();
        while let Ok((payload, rg)) = self.conn.recv(&read_buf, true).await {
            match MyCmd::my_deser(self.conn.ctx_mut(), &payload) {
                Err(e) => {
                    self.conn.send_err(e).await;
                }
                Ok((next, cmd)) => {
                    if !next.is_empty() {
                        self.conn.send_err(Error::MalformedPacket()).await;
                        continue;
                    }
                    if let Err(e) = self.handle_cmd(cmd).await {
                        self.conn.send_err(e).await;
                    }
                }
            }
            rg.advance(payload.len());
            self.conn.reset_pkt_nr();
        }
    }

    #[inline]
    async fn handle_cmd(&mut self, cmd: MyCmd<'_>) -> Result<()> {
        match cmd {
            MyCmd::Query(q) => {
                log::debug!("start processing query command: {}", q.query);
                let stmt = parse_stmt(MySQL(&q.query))?;
                let default_schema = self.curr_db.as_ref().map(|s| &s[..]).unwrap_or("");
                match stmt {
                    Statement::Select(qe) => {
                        match &qe.query {
                            Query::Row(..) => {
                                let _ = LgcPlan::new(&self.server.catalog, default_schema, &qe)?;
                                // // currently only support select row
                                // let _phy = PhyPlan::new(&lgc)
                                //     .map_err(|e| Error::PlanError(Box::new(e.to_string())))?;
                                todo!()
                            }
                            _ => return Err(Error::Unimplemented(Box::new("statement"))),
                        }
                    }
                    _ => {
                        // todo: implements other statements.
                        return Err(Error::Unimplemented(Box::new("statement")));
                    }
                }
            }
            MyCmd::FieldList(fl) => {
                log::debug!("start processing field list command: {:?}", fl);
            }
        }
        // always make sure buffer is flushed on success
        self.conn.flush().await
    }
}
