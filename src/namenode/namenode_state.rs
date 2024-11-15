use crate::block::Block;
use crate::errors::{CuddlyError, CuddlyResult};

use std::collections::HashMap;

const ALLOWED_CHARACTERS: &str = "=-_";

#[derive(Debug)]
enum IndexTreeNode {
    Directory {
        name: String,
        children: HashMap<String, IndexTreeNode>,
    },
    File {
        name: String,
        blocks: Vec<Block>,
    },
}

impl IndexTreeNode {
    fn add_directory(&mut self, name: &str) -> CuddlyResult<&mut IndexTreeNode> {
        match self {
            IndexTreeNode::Directory { name: _, children } => {
                let child = children
                    .entry(name.to_owned())
                    .or_insert(IndexTreeNode::Directory {
                        name: name.to_owned(),
                        children: HashMap::new(),
                    });
                Ok(child)
            }

            IndexTreeNode::File { name, blocks: _ } => Err(CuddlyError::FSError(format!(
                "'{}' s not a directory",
                name
            ))),
        }
    }

    fn get_name(&self) -> &str {
        match self {
            IndexTreeNode::Directory { name, children: _ } => name,
            IndexTreeNode::File { name, blocks: _ } => name,
        }
    }

    fn list(&self) -> Vec<&str> {
        match self {
            IndexTreeNode::Directory { name: _, children } => {
                children.values().map(|child| child.get_name()).collect()
            }
            IndexTreeNode::File { name, blocks: _ } => vec![name],
        }
    }
}

#[derive(Debug)]
pub struct NamenodeState {
    root: IndexTreeNode,
}

impl NamenodeState {
    pub fn new() -> Self {
        Self {
            root: IndexTreeNode::Directory {
                name: "".into(),
                children: HashMap::new(),
            },
        }
    }

    pub fn make_dir(&mut self, path: &str) -> CuddlyResult<()> {
        let path = starts_with_root_directory(path)?;
        let mut node = &mut self.root;
        for part in path.split('/') {
            if part.is_empty() {
                return Err(CuddlyError::FSError(
                    "Directory name cannot be empty".to_owned(),
                ));
            }
            if !is_valid_filename(part) {
                return Err(CuddlyError::FSError(format!(
                    "'{}' is not a valid directory name.",
                    part
                )));
            }
            node = node.add_directory(part)?;
        }

        Ok(())
    }

    pub fn list(&self, path: &str) -> CuddlyResult<Vec<&str>> {
        let path = starts_with_root_directory(path)?;
        if path.is_empty() {
            Ok(self.root.list())
        } else {
            let node = self.get_node(path)?;
            Ok(node.list())
        }
    }

    pub fn open_file(&self, path: &str) -> CuddlyResult<&[Block]> {
        let path = starts_with_root_directory(path)?;
        let node = self.get_node(path)?;

        match node {
            IndexTreeNode::Directory {
                name: _,
                children: _,
            } => Err(CuddlyError::FSError(format!(
                "'{}': Is not a file but a directory",
                path
            ))),
            IndexTreeNode::File { name: _, blocks } => Ok(blocks.as_slice()),
        }
    }

    pub fn create_file(&mut self, path: &str, blocks: &[Block]) -> CuddlyResult<()> {
        let path = starts_with_root_directory(path)?;

        let parts = path.split('/').collect::<Vec<_>>();
        let node = self.get_parent_node_mut(&parts)?;
        let filename = parts[parts.len() - 1];

        match node {
            IndexTreeNode::Directory {
                name: _,
                ref mut children,
            } => match children.get(filename) {
                Some(_) => Err(CuddlyError::FSError(format!(
                    "'{}': File or directory already exists",
                    filename
                ))),
                None => {
                    let file = IndexTreeNode::File {
                        name: filename.to_owned(),
                        blocks: blocks.into(),
                    };
                    children.insert(filename.to_owned(), file);
                    Ok(())
                }
            },
            IndexTreeNode::File { name, blocks: _ } => Err(CuddlyError::FSError(format!(
                "'{}': Directory expected, but got file",
                name
            ))),
        }
    }

    pub fn check_file_creation(&self, path: &str) -> CuddlyResult<()> {
        let path = starts_with_root_directory(path)?;
        if path.ends_with('/') {
            return Err(CuddlyError::FSError(
                "'/' is not a valid filename".to_owned(),
            ));
        }
        if path.is_empty() {
            return Err(CuddlyError::FSError(
                "Path and filename required".to_owned(),
            ));
        }

        let parts = path.split('/').collect::<Vec<_>>();
        let node = self.get_parent_node(&parts)?;
        let filename = parts[parts.len() - 1];

        match node {
            IndexTreeNode::Directory { name: _, children } => match children.get(filename) {
                Some(_) => Err(CuddlyError::FSError(format!(
                    "'{}': File or directory already exists",
                    filename
                ))),
                None => Ok(()),
            },
            IndexTreeNode::File { name, blocks: _ } => Err(CuddlyError::FSError(format!(
                "'{}': Directory expected, but got file",
                name
            ))),
        }
    }

    fn get_node(&self, path: &str) -> CuddlyResult<&IndexTreeNode> {
        let mut node = &self.root;
        for part in path.split('/') {
            match node {
                IndexTreeNode::Directory { name: _, children } => {
                    node = children.get(part).ok_or_else(|| {
                        CuddlyError::FSError(format!("'{}': No such file or directory", path))
                    })?;
                }
                IndexTreeNode::File { name: _, blocks: _ } => {
                    return Err(CuddlyError::FSError(format!("'{}': Not a directory", path)))
                }
            };
        }
        Ok(node)
    }

    fn get_parent_node(&self, parts: &[&str]) -> CuddlyResult<&IndexTreeNode> {
        let mut node = &self.root;

        for &part in parts.iter().take(parts.len() - 1) {
            node = match node {
                IndexTreeNode::Directory { name: _, children } => match children.get(part) {
                    Some(node) => node,
                    None => {
                        return Err(CuddlyError::FSError(format!(
                            "'{}': No such directory",
                            part
                        )))
                    }
                },
                IndexTreeNode::File { name, blocks: _ } => {
                    return Err(CuddlyError::FSError(format!(
                        "'{}': Directory expected, but got file",
                        name
                    )))
                }
            };
        }

        Ok(node)
    }

    fn get_parent_node_mut(&mut self, parts: &[&str]) -> CuddlyResult<&mut IndexTreeNode> {
        let mut node = &mut self.root;

        for &part in parts.iter().take(parts.len() - 1) {
            node = match node {
                IndexTreeNode::Directory { name: _, children } => match children.get_mut(part) {
                    Some(node) => node,
                    None => {
                        return Err(CuddlyError::FSError(format!(
                            "'{}': No such directory",
                            part
                        )))
                    }
                },
                IndexTreeNode::File { name, blocks: _ } => {
                    return Err(CuddlyError::FSError(format!(
                        "'{}': Directory expected, but got file",
                        name
                    )))
                }
            };
        }

        Ok(node)
    }
}

fn is_valid_filename(filename: &str) -> bool {
    filename
        .chars()
        .all(|c| char::is_alphanumeric(c) || ALLOWED_CHARACTERS.contains(c))
}

fn starts_with_root_directory(path: &str) -> CuddlyResult<&str> {
    path.strip_prefix("/")
        .ok_or_else(|| CuddlyError::FSError("Has to start with root directory".to_owned()))
}
