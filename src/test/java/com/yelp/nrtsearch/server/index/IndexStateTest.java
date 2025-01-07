/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.field.ObjectFieldDef;
import com.yelp.nrtsearch.server.field.TextFieldDef;
import org.junit.Test;

public class IndexStateTest {
  @Test
  public void testGetFieldBaseNestedPath_null() {
    IndexState mockState = mock(IndexState.class);
    try {
      IndexState.getFieldBaseNestedPath(null, mockState);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
  }

  @Test
  public void testGetFieldBaseNestedPath_root() {
    IndexState mockState = mock(IndexState.class);
    String path = IndexState.getFieldBaseNestedPath(IndexState.ROOT, mockState);
    assertNull(path);
  }

  @Test
  public void testGetFieldBaseNestedPath_inBaseDoc() {
    IndexState mockState = mock(IndexState.class);
    when(mockState.getFieldOrThrow("field")).thenReturn(mock(TextFieldDef.class));
    String path = IndexState.getFieldBaseNestedPath("field", mockState);
    assertEquals(IndexState.ROOT, path);
  }

  @Test
  public void testGetFieldBaseNestedPath_objectInBaseDoc() {
    IndexState mockState = mock(IndexState.class);
    ObjectFieldDef mockObject = mock(ObjectFieldDef.class);
    when(mockObject.isNestedDoc()).thenReturn(false);
    when(mockState.getFieldOrThrow("object")).thenReturn(mockObject);
    String path = IndexState.getFieldBaseNestedPath("object", mockState);
    assertEquals(IndexState.ROOT, path);
  }

  @Test
  public void testGetFieldBaseNestedPath_nestedObjectInBaseDoc() {
    IndexState mockState = mock(IndexState.class);
    ObjectFieldDef mockObject = mock(ObjectFieldDef.class);
    when(mockObject.isNestedDoc()).thenReturn(true);
    when(mockState.getFieldOrThrow("object")).thenReturn(mockObject);
    String path = IndexState.getFieldBaseNestedPath("object", mockState);
    assertEquals(IndexState.ROOT, path);
  }

  @Test
  public void testGetFieldBaseNestedPath_fieldOfObject() {
    IndexState mockState = mock(IndexState.class);
    ObjectFieldDef mockObject = mock(ObjectFieldDef.class);
    when(mockObject.isNestedDoc()).thenReturn(false);
    when(mockState.getFieldOrThrow("object")).thenReturn(mockObject);
    when(mockState.getFieldOrThrow("object.field")).thenReturn(mock(TextFieldDef.class));
    String path = IndexState.getFieldBaseNestedPath("object.field", mockState);
    assertEquals(IndexState.ROOT, path);
  }

  @Test
  public void testGetFieldBaseNestedPath_fieldOfNestedObject() {
    IndexState mockState = mock(IndexState.class);
    ObjectFieldDef mockObject = mock(ObjectFieldDef.class);
    when(mockObject.isNestedDoc()).thenReturn(true);
    when(mockState.getFieldOrThrow("object")).thenReturn(mockObject);
    when(mockState.getFieldOrThrow("object.field")).thenReturn(mock(TextFieldDef.class));
    String path = IndexState.getFieldBaseNestedPath("object.field", mockState);
    assertEquals("object", path);
  }

  @Test
  public void testGetFieldBaseNestedPath_multipleNestedObjects() {
    IndexState mockState = mock(IndexState.class);
    ObjectFieldDef mockObject = mock(ObjectFieldDef.class);
    when(mockObject.isNestedDoc()).thenReturn(true);
    when(mockState.getFieldOrThrow("object1")).thenReturn(mockObject);
    when(mockState.getFieldOrThrow("object1.object2")).thenReturn(mockObject);
    when(mockState.getFieldOrThrow("object1.object2.field")).thenReturn(mock(TextFieldDef.class));
    String path = IndexState.getFieldBaseNestedPath("object1.object2.field", mockState);
    assertEquals("object1.object2", path);
  }

  @Test
  public void testGetFieldBaseNestedPath_unknownField() {
    IndexState mockState = mock(IndexState.class);
    ObjectFieldDef mockObject = mock(ObjectFieldDef.class);
    when(mockObject.isNestedDoc()).thenReturn(true);
    when(mockState.getFieldOrThrow("object")).thenThrow(new IllegalArgumentException("error"));
    try {
      IndexState.getFieldBaseNestedPath("object.field", mockState);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("error", e.getMessage());
    }
  }
}
