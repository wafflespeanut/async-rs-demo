use failure::Fail;

macro_rules! impl_err_from {
    ($err:ident :: $type:ty > $variant:ident) => {
        impl From<$type> for $err {
            fn from(s: $type) -> Self {
                $err::$variant(s)
            }
        }
    };
}

#[derive(Debug, Fail)]
#[allow(dead_code)]
pub enum AggregatorError {
    #[fail(display = "Variable {} missing in environment.", _0)]
    MissingEnvVar(&'static str),
    #[fail(display = "Invalid value specified for {} in environment.", _0)]
    ParseEnvVar(&'static str),
    #[fail(display = "gRPC connection error: {}", _0)]
    RpcConnection(tonic::transport::Error),
    #[fail(display = "gRPC error: {}", _0)]
    RpcStatus(tonic::Status),
}

impl_err_from! {AggregatorError::tonic::transport::Error > RpcConnection}
impl_err_from! {AggregatorError::tonic::Status > RpcStatus}
