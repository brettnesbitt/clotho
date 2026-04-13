import re

with open("src/stream.rs", "r") as f:
    text = f.read()

# 1. AsyncTransformFn
text = text.replace(
    'type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;',
    '#[cfg(not(target_family = "wasm"))]\ntype AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;\n\n#[cfg(target_family = "wasm")]\ntype AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>>>>>;'
)

# 2. StreamPipeline
# Replace all "T: Send + Sync + 'static" across the file, but cautiously.
# Instead of blanket replacements, let's just create a StreamSend macro/alias or just use #[cfg] bounds for the methods.
# Actually, the simplest fix for branch() is to just rewrite make_branch_transform entirely.

old_branch_transform = """fn make_branch_transform<T, F, K>(predicate: F, sink: K) -> AsyncTransformFn<T>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> bool + Send + Sync + 'static,
    K: Sink<T> + Send + 'static,
{
    let sink: Arc<Mutex<K>> = Arc::new(Mutex::new(sink));
    Box::new(move |ctx: Context<T>| {
        let matches = predicate(&ctx.data);
        let sink: Arc<Mutex<K>> = Arc::clone(&sink);
        Box::pin(async move {
            if matches {
                let branch_ctx = Context {
                    data: ctx.data.clone(),
                    span_id: ctx.span_id.clone(),
                    parents: ctx.parents.clone(),
                    meta: ctx.meta.clone(),
                };
                let mut sink = sink.lock().await;
                if let Err(e) = sink.write(branch_ctx).await {
                    eprintln!("[Clotho] Branch sink write failed: {}", e);
                }
                Err((anyhow::anyhow!(BRANCH_SENTINEL), ctx))
            } else {
                Ok(ctx)
            }
        })
    })
}"""

new_branch_transform = """#[cfg(not(target_family = "wasm"))]
fn make_branch_transform<T, F, K>(predicate: F, sink: K) -> AsyncTransformFn<T>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> bool + Send + Sync + 'static,
    K: Sink<T> + Send + 'static,
{
    let sink: Arc<Mutex<K>> = Arc::new(Mutex::new(sink));
    Box::new(move |ctx: Context<T>| {
        let matches = predicate(&ctx.data);
        let sink: Arc<Mutex<K>> = Arc::clone(&sink);
        Box::pin(async move {
            if matches {
                let branch_ctx = Context {
                    data: ctx.data.clone(),
                    span_id: ctx.span_id.clone(),
                    parents: ctx.parents.clone(),
                    meta: ctx.meta.clone(),
                };
                let mut sink = sink.lock().await;
                if let Err(e) = sink.write(branch_ctx).await {
                    eprintln!("[Clotho] Branch sink write failed: {}", e);
                }
                Err((anyhow::anyhow!(BRANCH_SENTINEL), ctx))
            } else {
                Ok(ctx)
            }
        })
    })
}

#[cfg(target_family = "wasm")]
fn make_branch_transform<T, F, K>(predicate: F, sink: K) -> AsyncTransformFn<T>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> bool + Send + Sync + 'static,
    K: Sink<T> + 'static,
{
    // WASM does not need `Send` inside the Future, but Arc<Mutex> handles interior mutability.
    let sink: Arc<Mutex<K>> = Arc::new(Mutex::new(sink));
    Box::new(move |ctx: Context<T>| {
        let matches = predicate(&ctx.data);
        let sink: Arc<Mutex<K>> = Arc::clone(&sink);
        Box::pin(async move {
            if matches {
                let branch_ctx = Context {
                    data: ctx.data.clone(),
                    span_id: ctx.span_id.clone(),
                    parents: ctx.parents.clone(),
                    meta: ctx.meta.clone(),
                };
                let mut sink = sink.lock().await;
                if let Err(e) = sink.write(branch_ctx).await {
                    eprintln!("[Clotho] Branch sink write failed: {}", e);
                }
                Err((anyhow::anyhow!(BRANCH_SENTINEL), ctx))
            } else {
                Ok(ctx)
            }
        })
    })
}"""

if old_branch_transform in text:
    text = text.replace(old_branch_transform, new_branch_transform)
else:
    print("Could not find make_branch_transform")

# 3. branch function
old_branch = """    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + Send + 'static,
        T: Clone,"""

new_branch = """    #[cfg(not(target_family = "wasm"))]
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + Send + 'static,
        T: Clone,
    {
        let idx = self.next_step_idx();
        self.transforms.push(make_branch_transform(predicate, sink));
        self.transform_steps.push(StepInfo {
            name: format!("branch_{}", idx),
            step_type: "branch".to_string(),
        });
        self
    }

    #[cfg(target_family = "wasm")]
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + 'static,
        T: Clone,"""

if old_branch in text:
    text = text.replace(old_branch, new_branch)
    # The signature is replaced, but we basically keep the body for WASM directly after it if we just do:
    # Actually wait. If I replace the signature with the signature + both functions... I must strip the existing body for the first one manually in the regex unless I include the full body in the replace.
    # The full body is:
old_branch_full = """    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + Send + 'static,
        T: Clone,
    {
        let idx = self.next_step_idx();
        self.transforms.push(make_branch_transform(predicate, sink));
        self.transform_steps.push(StepInfo {
            name: format!("branch_{}", idx),
            step_type: "branch".to_string(),
        });
        self
    }"""
new_branch_full = """    #[cfg(not(target_family = "wasm"))]
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + Send + 'static,
        T: Clone,
    {
        let idx = self.next_step_idx();
        self.transforms.push(make_branch_transform(predicate, sink));
        self.transform_steps.push(StepInfo {
            name: format!("branch_{}", idx),
            step_type: "branch".to_string(),
        });
        self
    }

    #[cfg(target_family = "wasm")]
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + 'static,
        T: Clone,
    {
        let idx = self.next_step_idx();
        self.transforms.push(make_branch_transform(predicate, sink));
        self.transform_steps.push(StepInfo {
            name: format!("branch_{}", idx),
            step_type: "branch".to_string(),
        });
        self
    }"""
text = text.replace(old_branch_full, new_branch_full)

# 4. map_async
old_map_async = """    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send + 'static
    {
        let idx = self.next_step_idx();
        self.transforms.push(Box::new(move |ctx| {
            Box::pin(op(ctx))
        }));
        self.transform_steps.push(StepInfo {
            name: format!("map_async_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }"""

new_map_async = """    #[cfg(not(target_family = "wasm"))]
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send + 'static
    {
        let idx = self.next_step_idx();
        self.transforms.push(Box::new(move |ctx| {
            Box::pin(op(ctx))
        }));
        self.transform_steps.push(StepInfo {
            name: format!("map_async_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }

    #[cfg(target_family = "wasm")]
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + 'static
    {
        let idx = self.next_step_idx();
        self.transforms.push(Box::new(move |ctx| {
            Box::pin(op(ctx))
        }));
        self.transform_steps.push(StepInfo {
            name: format!("map_async_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }"""

text = text.replace(old_map_async, new_map_async)

with open("src/stream.rs", "w") as f:
    f.write(text)
print("Patch successfully applied!")
