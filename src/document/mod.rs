mod active_document;
mod lifecycle;

pub use crate::app_state::AppState;
pub use active_document::ActiveDocument;
pub use lifecycle::{close_document, get_or_create_doc};
