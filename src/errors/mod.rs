use actix_web::error::ResponseError;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use color_eyre::Report;
use core::fmt::Formatter;
use serde::{Serialize, Serializer};
use std::convert::From;
use tracing::error;

#[derive(Debug, Serialize)]
pub struct AppError {
    message: String,
    code: AppErrorCode,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AppErrorCode(i32);

impl AppErrorCode {
    pub fn message(self, _message: String) -> AppError {
        AppError {
            message: _message,
            code: self,
        }
    }

    pub fn default(self) -> AppError {
        let message = match self {
            _ => "An unexpected error has occurred.",
        };
        AppError {
            message: message.to_string(),
            code: self,
        }
    }
}

impl From<AppErrorCode> for AppError {
    fn from(error: AppErrorCode) -> Self {
        error.default()
    }
}

impl AppError {
    pub const INTERNAL_ERROR: AppErrorCode = AppErrorCode(1001);
}

impl Serialize for AppErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i32(self.0)
    }
}

impl From<Report> for AppError {
    fn from(e: Report) -> Self {
        error!("{:?}", e);
        Self::INTERNAL_ERROR.message("An unexpected error ocurred.".to_string())
    }
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self.code {
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(self)
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}
