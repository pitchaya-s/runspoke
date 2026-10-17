use std::{fs::File, process};

pub(crate) struct StateLock {
    file: File,
    owner: u32,
}

impl StateLock {
    pub(crate) fn new(file: File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock()?;
        Ok(Self {
            file,
            owner: process::id(),
        })
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        // A forked child must neither retain the owner's lock nor unlock it itself.
        if self.owner == process::id()
            && let Err(error) = self.file.unlock()
        {
            tracing::error!(%error, "failed to release state lock");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_owner_releases_lock_despite_inherited_handles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let open = || {
            File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let owner = StateLock::new(open()).unwrap();
        let inherited = owner.file.try_clone().unwrap();
        assert!(StateLock::new(open()).is_err());
        drop(owner);
        let replacement =
            StateLock::new(open()).expect("inherited descriptors must not retain ownership");
        drop(inherited);
        assert!(
            StateLock::new(open()).is_err(),
            "old descriptors must not release the replacement's lock"
        );
        drop(replacement);
        assert!(StateLock::new(open()).is_ok());
    }

    #[test]
    fn inherited_guard_cannot_unlock_another_process_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let open = || {
            File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let mut inherited_guard = StateLock::new(open()).unwrap();
        let owner_handle = inherited_guard.file.try_clone().unwrap();
        inherited_guard.owner = process::id().wrapping_add(1);
        drop(inherited_guard);
        assert!(
            StateLock::new(open()).is_err(),
            "a foreign destructor must preserve the owner's lock"
        );
        drop(owner_handle);
        assert!(StateLock::new(open()).is_ok());
    }
}
