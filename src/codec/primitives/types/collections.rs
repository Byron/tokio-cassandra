use super::cql_string::CqlString;
use std::collections::HashMap;
use bytes::BytesMut;
use super::*;



/// TODO: zero copy - implement it as offset to beginning of vec to CqlStrings to prevent Vec
/// allocation
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CqlStringList<T>
    where T: AsRef<[u8]>
{
    container: Vec<CqlString<T>>,
}


impl<T> CqlFrom<CqlStringList<T>, Vec<CqlString<T>>> for CqlStringList<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    unsafe fn unchecked_from(lst: Vec<CqlString<T>>) -> CqlStringList<T> {
        CqlStringList { container: lst }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl CqlStringList<BytesMut> {
    pub fn try_from_iter_easy<'a, I, E, S>(v: I) -> Result<CqlStringList<BytesMut>>
        where I: IntoIterator<IntoIter = E, Item = S>,
              E: Iterator<Item = S> + ExactSizeIterator,
              S: Into<&'a str>
    {
        let v = v.into_iter();
        let mut res = Vec::with_capacity(v.len());
        for s in v {
            res.push(CqlString::try_from(s.into())?);
        }
        CqlStringList::try_from(res)
    }
}

impl CqlStringList<Vec<u8>> {
    pub fn try_from_iter<'a, I, E, S>(v: I) -> Result<CqlStringList<Vec<u8>>>
        where I: IntoIterator<IntoIter = E, Item = S>,
              E: Iterator<Item = S> + ExactSizeIterator,
              S: Into<&'a str>
    {
        let v = v.into_iter();
        let mut res = Vec::with_capacity(v.len());
        for s in v {
            res.push(CqlString::try_from(s.into())?);
        }
        CqlStringList::try_from(res)
    }
}

impl<T> CqlStringList<T>
    where T: AsRef<[u8]>
{
    pub fn len(&self) -> u16 {
        self.container.len() as u16
    }

    pub fn iter(&self) -> ::std::slice::Iter<CqlString<T>> {
        self.container.iter()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CqlStringMap<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    container: HashMap<CqlString<T>, CqlString<T>>,
}

impl<T> CqlFrom<CqlStringMap<T>, HashMap<CqlString<T>, CqlString<T>>> for CqlStringMap<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    unsafe fn unchecked_from(map: HashMap<CqlString<T>, CqlString<T>>) -> CqlStringMap<T> {
        CqlStringMap { container: map }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl<T> CqlStringMap<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    pub fn try_from_iter<I, E>(v: I) -> Result<CqlStringMap<T>>
        where I: IntoIterator<IntoIter = E, Item = (CqlString<T>, CqlString<T>)>,
              E: Iterator<Item = (CqlString<T>, CqlString<T>)> + ExactSizeIterator
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

    pub fn iter(&self) -> ::std::collections::hash_map::Iter<CqlString<T>, CqlString<T>> {
        self.container.iter()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CqlStringMultiMap<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    container: HashMap<CqlString<T>, CqlStringList<T>>,
}

impl<T> CqlFrom<CqlStringMultiMap<T>, HashMap<CqlString<T>, CqlStringList<T>>> for CqlStringMultiMap<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    unsafe fn unchecked_from(map: HashMap<CqlString<T>, CqlStringList<T>>) -> CqlStringMultiMap<T> {
        CqlStringMultiMap { container: map }
    }

    fn max_len() -> usize {
        u16::max_value() as usize
    }
}

impl<T> CqlStringMultiMap<T>
    where T: AsRef<[u8]> + PartialEq + Eq
{
    pub fn try_from_iter<I, E>(v: I) -> Result<CqlStringMultiMap<T>>
        where I: IntoIterator<IntoIter = E, Item = (CqlString<T>, CqlStringList<T>)>,
              E: Iterator<Item = (CqlString<T>, CqlStringList<T>)> + ExactSizeIterator
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

    pub fn iter(&self) -> ::std::collections::hash_map::Iter<CqlString<T>, CqlStringList<T>> {
        self.container.iter()
    }

    pub fn get(&self, k: &CqlString<T>) -> Option<&CqlStringList<T>> {
        self.container.get(k)
    }
}
