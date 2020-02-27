use failure::Fail;

#[derive(Debug, Fail)]
#[allow(dead_code)]
pub enum AggregatorError {
    #[fail(display = "Variable {} missing in environment.", _0)]
    MissingEnvVar(&'static str),
    #[fail(display = "Invalid value specified for {} in environment.", _0)]
    ParseEnvVar(&'static str),
    #[fail(display = "gRPC error: {}", _0)]
    Rpc(tonic::transport::Error),
}
