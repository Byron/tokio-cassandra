use bytes::{BufMut, BytesMut};

error_chain! {
    errors {
        UnknownAuthenticator(auth: String)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Credentials {
    Login { username: String, password: String },
}


pub enum Authenticator {
    PlainTextAuthenticator { username: String, password: String },
}

impl Authenticator {
    pub fn from_name(name: &str, credentials: Credentials) -> Result<Authenticator> {
        use self::Credentials::*;
        match name {
            "org.apache.cassandra.auth.PasswordAuthenticator" => {
                match credentials {
                    Login {
                        username: user,
                        password: pwd,
                    } => {
                        Ok(Authenticator::PlainTextAuthenticator {
                               username: user,
                               password: pwd,
                           })
                    }
                }
            }
            _ => Err(ErrorKind::UnknownAuthenticator(name.to_string()).into()),
        }
    }

    pub fn encode_auth_response<'a>(&self, v: &mut BytesMut) -> () {
        match self {
            &Authenticator::PlainTextAuthenticator {
                 username: ref user,
                 password: ref pwd,
             } => {
                v.put_u8(0x00);
                v.extend(user.as_bytes());
                v.put_u8(0x00);
                v.extend(pwd.as_bytes());
            }
        }

    }
}


#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;


    #[test]
    fn plain_text_auth() {
        let srv_auth = "org.apache.cassandra.auth.PasswordAuthenticator";
        let auth = Authenticator::from_name(srv_auth,
                                            Credentials::Login {
                                                username: String::new(),
                                                password: String::new(),
                                            });

        use super::Authenticator::*;

        match auth.unwrap() {
            PlainTextAuthenticator { .. } => (),
        }
    }

    #[test]
    fn unknown_auth() {
        let srv_auth = "unknown";
        let auth = Authenticator::from_name(srv_auth,
                                            Credentials::Login {
                                                username: String::new(),
                                                password: String::new(),
                                            });

        assert!(auth.is_err());
    }

    #[test]
    fn plain_text_encode() {
        let auth = Authenticator::PlainTextAuthenticator {
            username: String::from("abcuser"),
            password: String::from("abcpass"),
        };

        let mut encoded = BytesMut::with_capacity(64);
        auth.encode_auth_response(&mut encoded);

        let expected = &[0u8, 97, 98, 99, 117, 115, 101, 114, 0, 97, 98, 99, 112, 97, 115, 115];

        assert_eq!(&encoded[..], &expected[..]);
    }

}
