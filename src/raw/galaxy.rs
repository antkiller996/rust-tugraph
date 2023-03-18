// Copyright 2023 antkiller
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use libc::c_char;
use std::{collections::HashMap, ptr, slice};

use crate::{
    ffi,
    raw::ffi_util::{to_rust_string, CStrLike},
    types::AccessLevel,
    Error,
};

use super::{db::RawGraphDB, RawRoleInfo, RawUserInfo};

pub(crate) struct RawGalaxy {
    inner: *mut ffi::lgraph_api_galaxy_t,
}

impl Drop for RawGalaxy {
    fn drop(&mut self) {
        unsafe {
            ffi_try! {
                ffi::lgraph_api_galaxy_close(self.inner)
            }
            .expect("failed to close galaxy close");
            ffi::lgraph_api_galaxy_destroy(self.inner);
        }
    }
}

impl RawGalaxy {
    pub(crate) fn new<D: CStrLike>(
        dir: D,
        durable: bool,
        create_if_not_exist: bool,
    ) -> Result<Self, Error> {
        unsafe {
            let cdir = dir.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_create(cdir.as_ptr(), durable, create_if_not_exist)
            }
            .map(|ptr| RawGalaxy { inner: ptr })
        }
    }

    pub(crate) fn new_with_user<D: CStrLike, T: CStrLike>(
        dir: D,
        username: T,
        password: T,
        durable: bool,
        create_if_not_exist: bool,
    ) -> Result<Self, Error> {
        unsafe {
            let cdir = dir.into_c_string().unwrap();
            let cusername = username.into_c_string().unwrap();
            let cpassword = password.into_c_string().unwrap();
            ffi_try! {ffi::lgraph_api_galaxy_create_with_user(
                cdir.as_ptr(),
                cusername.as_ptr(),
                cpassword.as_ptr(),
                durable,
                create_if_not_exist,
            )}
            .map(|ptr| RawGalaxy { inner: ptr })
        }
    }

    pub(crate) fn set_current_user<T: CStrLike>(&self, user: T, password: T) -> Result<(), Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            let cpassword = password.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_set_current_user(self.inner, cuser.as_ptr(), cpassword.as_ptr())
            }
        }
    }

    pub(crate) fn set_user<T: CStrLike>(&self, user: T) -> Result<(), Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_set_user(self.inner, cuser.as_ptr())
            }
        }
    }

    pub(crate) fn create_graph<T: CStrLike>(
        &self,
        graph_name: T,
        description: T,
        max_size: usize,
    ) -> Result<bool, Error> {
        unsafe {
            let cgraph_name = graph_name.into_c_string().unwrap();
            let cdescription = description.into_c_string().unwrap();
            ffi_try! {ffi::lgraph_api_galaxy_create_graph(
                self.inner,
                cgraph_name.as_ptr(),
                cdescription.as_ptr(),
                max_size,
            )}
        }
    }

    pub(crate) fn delete_graph<T: CStrLike>(&self, graph_name: T) -> Result<bool, Error> {
        unsafe {
            let cgraph_name = graph_name.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_delete_graph(
                self.inner,
                cgraph_name.as_ptr()
            ))
        }
    }

    pub(crate) fn mod_graph<T: CStrLike>(
        &self,
        graph_name: T,
        mod_desc: bool,
        desc: T,
        mod_size: bool,
        new_max_size: usize,
    ) -> Result<bool, Error> {
        unsafe {
            let cgraph_name = graph_name.into_c_string().unwrap();
            let cdesc = desc.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_mod_graph(
                self.inner,
                cgraph_name.as_ptr(),
                mod_desc,
                cdesc.as_ptr(),
                mod_size,
                new_max_size
            ))
        }
    }

    pub(crate) fn list_graphs(&self) -> Result<HashMap<String, (String, usize)>, Error> {
        unsafe {
            let mut cgraph_names: *mut *mut c_char = ptr::null_mut();
            let mut cgraph_descs: *mut *mut c_char = ptr::null_mut();
            let mut cgraph_sizes: *mut usize = ptr::null_mut();
            let len = ffi_try! {ffi::lgraph_api_galaxy_list_graphs(
                self.inner,
                &mut cgraph_names as *mut _,
                &mut cgraph_descs as *mut _,
                &mut cgraph_sizes as *mut _,
            )}?;
            let cgraph_names: Vec<_> = slice::from_raw_parts(cgraph_names, len)
                .iter()
                .map(|ptr| to_rust_string(*ptr))
                .collect();
            let cgraph_descs: Vec<_> = slice::from_raw_parts(cgraph_descs, len)
                .iter()
                .map(|ptr| to_rust_string(*ptr))
                .collect();
            let cgraph_sizes = slice::from_raw_parts(cgraph_sizes, len);

            let m = cgraph_names
                .into_iter()
                .zip(cgraph_descs.into_iter().zip(cgraph_sizes.iter().copied()))
                .collect();
            Ok(m)
        }
    }

    pub(crate) fn create_user<T: CStrLike>(
        &self,
        user: T,
        password: T,
        desc: T,
    ) -> Result<bool, Error> {
        unsafe {
            let cuser_name = user.into_c_string().unwrap();
            let cpassword = password.into_c_string().unwrap();
            let cdesc = desc.into_c_string().unwrap();
            ffi_try! {ffi::lgraph_api_galaxy_create_user(
                self.inner,
                cuser_name.as_ptr(),
                cpassword.as_ptr(),
                cdesc.as_ptr(),
            )}
        }
    }

    pub(crate) fn delete_user<T: CStrLike>(&self, user: T) -> Result<bool, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            ffi_try! {ffi::lgraph_api_galaxy_delete_user(
                self.inner,
                cuser.as_ptr(),
            )}
        }
    }

    pub(crate) fn set_password<T: CStrLike>(
        &self,
        user: T,
        old_password: T,
        new_password: T,
    ) -> Result<bool, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            let old_password = old_password.into_c_string().unwrap();
            let new_password = new_password.into_c_string().unwrap();
            ffi_try! {ffi::lgraph_api_galaxy_set_password(
                self.inner,
                cuser.as_ptr(),
                old_password.as_ptr(),
                new_password.as_ptr(),
            )}
        }
    }

    pub(crate) fn set_user_desc<T: CStrLike>(&self, user: T, desc: T) -> Result<bool, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            let cdesc = desc.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_set_user_desc(self.inner, cuser.as_ptr(), cdesc.as_ptr())
            }
        }
    }

    pub(crate) fn set_user_roles<T, D, R>(&self, user: T, roles: R) -> Result<bool, Error>
    where
        T: CStrLike,
        D: CStrLike + Copy,
        R: IntoIterator<Item = D>,
    {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            let croles: Vec<_> = roles
                .into_iter()
                .map(|r| r.into_c_string().unwrap())
                .collect();
            let croles: Vec<_> = croles.iter().map(|r| r.as_ptr()).collect();
            ffi_try! {
                    ffi::lgraph_api_galaxy_set_user_roles(
                    self.inner,
                    cuser.as_ptr(),
                    croles.as_ptr(),
                    croles.len(),
                )
            }
        }
    }

    pub(crate) fn set_user_graph_access<T: CStrLike>(
        &self,
        user: T,
        graph: T,
        access: AccessLevel,
    ) -> Result<bool, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            let cgraph = graph.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_set_user_graph_access(
                self.inner,
                cuser.as_ptr(),
                cgraph.as_ptr(),
                access as isize as i32,
            ))
        }
    }

    pub(crate) fn disable_user<T: CStrLike>(&self, user: T) -> Result<bool, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_disable_user(
                self.inner,
                cuser.as_ptr(),
            ))
        }
    }

    pub(crate) fn enable_user<T: CStrLike>(&self, user: T) -> Result<bool, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_enable_user(self.inner, cuser.as_ptr())
            }
        }
    }

    pub(crate) fn list_users(&self) -> Result<HashMap<String, RawUserInfo>, Error> {
        unsafe {
            let mut cuser_names: *mut *mut c_char = ptr::null_mut();
            let mut cuser_infos: *mut *mut ffi::lgraph_api_user_info_t = ptr::null_mut();
            let len = ffi_try! {ffi::lgraph_api_galaxy_list_users(
                self.inner,
                &mut cuser_names as *mut _,
                &mut cuser_infos as *mut _,
            )}?;
            let user_names: Vec<_> = slice::from_raw_parts(cuser_names, len)
                .iter()
                .map(|ptr| to_rust_string(*ptr))
                .collect();
            let user_infos: Vec<_> = slice::from_raw_parts(cuser_infos, len)
                .iter()
                .map(|ptr| RawUserInfo::from_ptr(*ptr))
                .collect();
            let m = user_names.into_iter().zip(user_infos.into_iter()).collect();
            Ok(m)
        }
    }

    pub(crate) fn get_user_info<T: CStrLike>(&self, user: T) -> Result<RawUserInfo, Error> {
        let cuser = user.into_c_string().unwrap();
        unsafe {
            ffi_try!(ffi::lgraph_api_galaxy_get_user_info(
                self.inner,
                cuser.as_ptr()
            ))
            .map(|ptr| unsafe { RawUserInfo::from_ptr(ptr) })
        }
    }

    pub(crate) fn create_role<T: CStrLike>(&self, role: T, desc: T) -> Result<bool, Error> {
        unsafe {
            let crole = role.into_c_string().unwrap();
            let cdesc = desc.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_create_role(
                self.inner,
                crole.as_ptr(),
                cdesc.as_ptr()
            ))
        }
    }

    pub(crate) fn delete_role<T: CStrLike>(&self, role: T) -> Result<bool, Error> {
        unsafe {
            let crole = role.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_delete_role(
                self.inner,
                crole.as_ptr()
            ))
        }
    }

    pub(crate) fn disable_role<T: CStrLike>(&self, role: T) -> Result<bool, Error> {
        unsafe {
            let crole = role.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_disable_role(
                self.inner,
                crole.as_ptr()
            ))
        }
    }

    pub(crate) fn enable_role<T: CStrLike>(&self, role: T) -> Result<bool, Error> {
        unsafe {
            let crole = role.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_enable_role(
                self.inner,
                crole.as_ptr()
            ))
        }
    }

    pub(crate) fn set_role_desc<T: CStrLike>(&self, role: T, desc: T) -> Result<bool, Error> {
        unsafe {
            let crole = role.into_c_string().unwrap();
            let cdesc = desc.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_set_role_desc(self.inner, crole.as_ptr(), cdesc.as_ptr())
            }
        }
    }

    pub(crate) fn set_role_access_rights<'a, T, R>(
        &self,
        role: T,
        graph_access: R,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        R: Iterator<Item = (&'a str, AccessLevel)>,
    {
        unsafe {
            let crole = role.into_c_string().unwrap();
            let (cgraph_names, caccess_levels): (Vec<_>, Vec<_>) = graph_access
                .map(|(n, l)| (n.into_c_string().unwrap(), l as isize as i32))
                .unzip();
            let cgraph_names: Vec<_> = cgraph_names.iter().map(|n| n.as_ptr()).collect();
            ffi_try!(ffi::lgraph_api_galaxy_set_role_access_rights(
                self.inner,
                crole.as_ptr(),
                cgraph_names.as_ptr(),
                caccess_levels.as_ptr(),
                cgraph_names.len(),
            ))
        }
    }

    pub(crate) fn set_role_access_rights_incremental<'a, T, R>(
        &self,
        role: T,
        graph_access: R,
    ) -> Result<bool, Error>
    where
        T: CStrLike,
        R: Iterator<Item = (&'a str, AccessLevel)>,
    {
        unsafe {
            let crole = role.into_c_string().unwrap();
            let (cgraph_names, caccess_levels): (Vec<_>, Vec<_>) = graph_access
                .map(|(n, l)| (n.into_c_string().unwrap(), l as isize as i32))
                .unzip();
            let cgraph_names: Vec<_> = cgraph_names.iter().map(|n| n.as_ptr()).collect();
            ffi_try!(ffi::lgraph_api_galaxy_set_role_access_rights_incremental(
                self.inner,
                crole.as_ptr(),
                cgraph_names.as_ptr(),
                caccess_levels.as_ptr(),
                cgraph_names.len(),
            ))
        }
    }

    pub(crate) fn get_role_info<T: CStrLike>(&self, role: T) -> Result<RawRoleInfo, Error> {
        unsafe {
            let crole = role.into_c_string().unwrap();
            ffi_try!(ffi::lgraph_api_galaxy_get_role_info(
                self.inner,
                crole.as_ptr()
            ))
            .map(|ptr| unsafe { RawRoleInfo::from_ptr(ptr) })
        }
    }

    pub(crate) fn list_roles(&self) -> Result<HashMap<String, RawRoleInfo>, Error> {
        unsafe {
            let mut crole_names: *mut *mut c_char = ptr::null_mut();
            let mut crole_infos: *mut *mut ffi::lgraph_api_role_info_t = ptr::null_mut();
            let len = ffi_try!(ffi::lgraph_api_galaxy_list_roles(
                self.inner,
                &mut crole_names as *mut _,
                &mut crole_infos as *mut _,
            ))?;
            let croles_names: Vec<_> = slice::from_raw_parts(crole_names, len)
                .iter()
                .map(|ptr| to_rust_string(*ptr))
                .collect();
            let croles_infos: Vec<_> = slice::from_raw_parts(crole_infos, len)
                .iter()
                .map(|ptr| RawRoleInfo::from_ptr(*ptr))
                .collect();
            let m = croles_names
                .into_iter()
                .zip(croles_infos.into_iter())
                .collect();
            Ok(m)
        }
    }

    pub(crate) fn get_access_level<T: CStrLike>(
        &self,
        user: T,
        graph: T,
    ) -> Result<AccessLevel, Error> {
        unsafe {
            let cuser = user.into_c_string().unwrap();
            let cgraph = graph.into_c_string().unwrap();
            ffi_try! {
                ffi::lgraph_api_galaxy_get_access_level(self.inner, cuser.as_ptr(), cgraph.as_ptr())
            }
            .and_then(|l| AccessLevel::try_from(l as u32))
        }
    }

    pub(crate) unsafe fn open_graph<T: CStrLike>(
        &self,
        graph: T,
        read_only: bool,
    ) -> Result<RawGraphDB, Error> {
        let cgraph = graph.into_c_string().unwrap();
        ffi_try! {
            ffi::lgraph_api_galaxy_open_graph(self.inner, cgraph.as_ptr(), read_only)
        }
        .map(|ptr| unsafe { RawGraphDB::from_ptr(ptr) })
    }
}
