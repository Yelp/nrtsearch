/*
 * Copyright 2020 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.plugins;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;

/**
 * Custom ClassLoader used to load Plugins. Allows the option of overriding things like permissions
 * and class name loading.
 */
public class PluginClassLoader extends URLClassLoader {

  PluginClassLoader(URL[] urls) {
    super(urls);
  }

  // This method gets called by the SecureClassLoader parent class to assign a
  // set of permissions to a certain CodeSource. We only load the trusted
  // module code in this classloader, so we always grant all of the
  // permissions to the loaded code.
  @Override
  protected PermissionCollection getPermissions(CodeSource codesource) {
    final Permissions permissions = new Permissions();
    permissions.add(new AllPermission());
    return permissions;
  }
}
