use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Clone, Copy)]
pub enum Method {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl Method {
    fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Patch => "PATCH",
            Method::Delete => "DELETE",
        }
    }
}

#[derive(Clone, Default)]
pub struct Client {
    #[cfg(not(target_family = "wasm"))]
    inner: reqwest::Client,
}

impl Client {
    pub fn new() -> Self {
        #[cfg(not(target_family = "wasm"))]
        {
            Self {
                inner: reqwest::Client::new(),
            }
        }

        #[cfg(target_family = "wasm")]
        {
            Self {}
        }
    }

    pub fn get(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, Method::Get, url)
    }

    pub fn post(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, Method::Post, url)
    }

    pub fn put(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, Method::Put, url)
    }

    pub fn patch(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, Method::Patch, url)
    }

    pub fn delete(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, Method::Delete, url)
    }
}

pub struct RequestBuilder<'a> {
    client: &'a Client,
    method: Method,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl<'a> RequestBuilder<'a> {
    fn new(client: &'a Client, method: Method, url: &str) -> Self {
        Self {
            client,
            method,
            url: url.to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    pub fn json<T: Serialize>(mut self, value: &T) -> Result<Self> {
        self.body = serde_json::to_vec(value)?;
        if !self
            .headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        {
            self.headers
                .push(("content-type".to_string(), "application/json".to_string()));
        }
        Ok(self)
    }

    pub async fn send(self) -> Result<Response> {
        #[cfg(target_family = "wasm")]
        {
            use spin_sdk::http::{send, Request};

            let mut req_builder = Request::builder()
                .method(self.method.as_str())
                .uri(&self.url);

            for (key, value) in self.headers {
                req_builder = req_builder.header(key, value);
            }

            let req = req_builder.body(self.body)?;
            let res = send(req).await?;

            let status = res.status().as_u16();
            let headers = res
                .headers()
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|vv| (k.to_string(), vv.to_string())))
                .collect();

            Ok(Response {
                status,
                headers,
                body: res.body().to_vec(),
            })
        }

        #[cfg(not(target_family = "wasm"))]
        {
            let mut req = self
                .client
                .inner
                .request(
                    reqwest::Method::from_bytes(self.method.as_str().as_bytes())?,
                    &self.url,
                );

            for (key, value) in self.headers {
                req = req.header(key, value);
            }

            if !self.body.is_empty() {
                req = req.body(self.body);
            }

            let res = req.send().await?;
            let status = res.status().as_u16();
            let headers = res
                .headers()
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|vv| (k.to_string(), vv.to_string())))
                .collect();
            let body = res.bytes().await?.to_vec();

            Ok(Response {
                status,
                headers,
                body,
            })
        }
    }
}

pub struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn bytes(&self) -> &[u8] {
        &self.body
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.body
    }

    pub fn text(&self) -> Result<String> {
        Ok(String::from_utf8(self.body.clone())?)
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.body)?)
    }
}
