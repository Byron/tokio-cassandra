use super::cql_string::CqlString;
use std::collections::HashMap;
use super::*;


/// TODO: zero copy - implement it as offset to beginning of vec to CqlStrings to prevent Vec
/// allocation
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CqlStringList {
    container: Vec<CqlString>,
}


impl CqlFrom<CqlStringList, Vec<CqlString>> for CqlStringList {
    unsafe fn unchecked_from(lst: Vec<CqlString>) -> CqlStringList {
        CqlStringList { container: lst }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl CqlStringList {
    pub fn try_from_iter_easy<'a, I, E, S>(v: I) -> Result<CqlStringList>
    where
        I: IntoIterator<IntoIter = E, Item = S>,
        E: Iterator<Item = S> + ExactSizeIterator,
        S: Into<&'a str>,
    {
        let v = v.into_iter();
        let mut res = Vec::with_capacity(v.len());
        for s in v {
            res.push(CqlString::try_from(s.into())?);
        }
        CqlStringList::try_from(res)
    }
}

impl CqlStringList {
    pub fn try_from_iter<'a, I, E, S>(v: I) -> Result<CqlStringList>
    where
        I: IntoIterator<IntoIter = E, Item = S>,
        E: Iterator<Item = S> + ExactSizeIterator,
        S: Into<&'a str>,
    {
        let v = v.into_iter();
        let mut res = Vec::with_capacity(v.len());
        for s in v {
            res.push(CqlString::try_from(s.into())?);
        }
        CqlStringList::try_from(res)
    }
}

impl CqlStringList {
    pub fn len(&self) -> u16 {
        self.container.len() as u16
    }

    pub fn iter(&self) -> ::std::slice::Iter<CqlString> {
        self.container.iter()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CqlStringMap {
    container: HashMap<CqlString, CqlString>,
}

impl CqlFrom<CqlStringMap, HashMap<CqlString, CqlString>> for CqlStringMap {
    unsafe fn unchecked_from(map: HashMap<CqlString, CqlString>) -> CqlStringMap {
        CqlStringMap { container: map }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl CqlStringMap {
    pub fn try_from_iter<I, E>(v: I) -> Result<CqlStringMap>
    where
        I: IntoIterator<IntoIter = E, Item = (CqlString, CqlString)>,
        E: Iterator<Item = (CqlString, CqlString)> + ExactSizeIterator,
    {
        let v = v.into_iter();
        let mut res = HashMap::with_capacity(v.len());
        for (k, v) in v {
            res.insert(k, v);
        }
        CqlStringMap::try_from(res)
    }

    pub fn len(&self) -> u16 {
        self.container.len() as u16
    }

    pub fn iter(&self) -> ::std::collections::hash_map::Iter<CqlString, CqlString> {
        self.container.iter()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CqlStringMultiMap {
    container: HashMap<CqlString, CqlStringList>,
}

impl CqlFrom<CqlStringMultiMap, HashMap<CqlString, CqlStringList>> for CqlStringMultiMap {
    unsafe fn unchecked_from(map: HashMap<CqlString, CqlStringList>) -> CqlStringMultiMap {
        CqlStringMultiMap { container: map }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl CqlStringMultiMap {
    pub fn try_from_iter<I, E>(v: I) -> Result<CqlStringMultiMap>
    where
        I: IntoIterator<IntoIter = E, Item = (CqlString, CqlStringList)>,
        E: Iterator<Item = (CqlString, CqlStringList)> + ExactSizeIterator,
    {
        let v = v.into_iter();
        let mut res = HashMap::with_capacity(v.len());
        for (k, v) in v {
            res.insert(k, v);
        }
        CqlStringMultiMap::try_from(res)
    }

    pub fn len(&self) -> u16 {
        self.container.len() as u16
    }

    pub fn iter(&self) -> ::std::collections::hash_map::Iter<CqlString, CqlStringList> {
        self.container.iter()
    }

    pub fn get(&self, k: &CqlString) -> Option<&CqlStringList> {
        self.container.get(k)
    }
}
