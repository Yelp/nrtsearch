/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.doc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReaderContext;
import org.junit.Before;
import org.junit.Test;

public class DocLookupTest {

  private Function<String, FieldDef> mockFieldDefLookup;
  private Supplier<Collection<String>> mockAllFieldNamesSupplier;
  private FieldDef mockFieldDef1;
  private FieldDef mockFieldDef2;
  private DocLookup docLookup;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    mockFieldDefLookup = mock(Function.class);
    mockAllFieldNamesSupplier = mock(Supplier.class);
    mockFieldDef1 = mock(FieldDef.class);
    mockFieldDef2 = mock(FieldDef.class);

    when(mockFieldDef1.getName()).thenReturn("field1");
    when(mockFieldDef1.getType()).thenReturn("TEXT");
    when(mockFieldDef1.getFacetValueType()).thenReturn(IndexableFieldDef.FacetValueType.NO_FACETS);

    when(mockFieldDef2.getName()).thenReturn("field2");
    when(mockFieldDef2.getType()).thenReturn("INT");
    when(mockFieldDef2.getFacetValueType()).thenReturn(IndexableFieldDef.FacetValueType.NO_FACETS);

    docLookup = new DocLookup(mockFieldDefLookup, mockAllFieldNamesSupplier);
  }

  @Test
  public void testGetFieldDefOrThrow_ExistingField() {
    // Arrange
    when(mockFieldDefLookup.apply("field1")).thenReturn(mockFieldDef1);

    // Act
    FieldDef result = docLookup.getFieldDefOrThrow("field1");

    // Assert
    assertEquals(mockFieldDef1, result);
    verify(mockFieldDefLookup).apply("field1");
  }

  @Test
  public void testGetFieldDefOrThrow_NonExistentField() {
    // Arrange
    when(mockFieldDefLookup.apply("nonexistent")).thenReturn(null);

    // Act & Assert
    try {
      docLookup.getFieldDefOrThrow("nonexistent");
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("field \"nonexistent\" is unknown", e.getMessage());
    }
    verify(mockFieldDefLookup).apply("nonexistent");
  }

  @Test
  public void testGetFieldDefOrThrow_NullFieldName() {
    // Arrange
    when(mockFieldDefLookup.apply(null)).thenReturn(null);

    // Act & Assert
    try {
      docLookup.getFieldDefOrThrow(null);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("field \"null\" is unknown", e.getMessage());
    }
    verify(mockFieldDefLookup).apply(null);
  }

  @Test
  public void testGetFieldDefOrThrow_EmptyFieldName() {
    // Arrange
    when(mockFieldDefLookup.apply("")).thenReturn(null);

    // Act & Assert
    try {
      docLookup.getFieldDefOrThrow("");
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("field \"\" is unknown", e.getMessage());
    }
    verify(mockFieldDefLookup).apply("");
  }

  @Test
  public void testGetAllFieldNames_WithFields() {
    // Arrange
    Collection<String> expectedFieldNames = Arrays.asList("field1", "field2", "field3");
    when(mockAllFieldNamesSupplier.get()).thenReturn(expectedFieldNames);

    // Act
    Collection<String> result = docLookup.getAllFieldNames();

    // Assert
    assertEquals(expectedFieldNames, result);
    verify(mockAllFieldNamesSupplier).get();
  }

  @Test
  public void testGetAllFieldNames_EmptyCollection() {
    // Arrange
    Collection<String> emptyFieldNames = new HashSet<>();
    when(mockAllFieldNamesSupplier.get()).thenReturn(emptyFieldNames);

    // Act
    Collection<String> result = docLookup.getAllFieldNames();

    // Assert
    assertTrue(result.isEmpty());
    assertEquals(emptyFieldNames, result);
    verify(mockAllFieldNamesSupplier).get();
  }

  @Test
  public void testGetAllFieldNames_NullCollection() {
    // Arrange
    when(mockAllFieldNamesSupplier.get()).thenReturn(null);

    // Act
    Collection<String> result = docLookup.getAllFieldNames();

    // Assert
    assertNull(result);
    verify(mockAllFieldNamesSupplier).get();
  }

  @Test
  public void testGetAllFieldNames_MultipleCalls() {
    // Arrange
    Collection<String> fieldNames1 = Arrays.asList("field1", "field2");
    Collection<String> fieldNames2 = Arrays.asList("field1", "field2", "field3");
    when(mockAllFieldNamesSupplier.get()).thenReturn(fieldNames1, fieldNames2);

    // Act
    Collection<String> result1 = docLookup.getAllFieldNames();
    Collection<String> result2 = docLookup.getAllFieldNames();

    // Assert
    assertEquals(fieldNames1, result1);
    assertEquals(fieldNames2, result2);
    verify(mockAllFieldNamesSupplier, times(2)).get();
  }

  @Test
  public void testGetFieldDef_ExistingField() {
    // Arrange
    when(mockFieldDefLookup.apply("field1")).thenReturn(mockFieldDef1);

    // Act
    FieldDef result = docLookup.getFieldDef("field1");

    // Assert
    assertEquals(mockFieldDef1, result);
    verify(mockFieldDefLookup).apply("field1");
  }

  @Test
  public void testGetFieldDef_NonExistentField() {
    // Arrange
    when(mockFieldDefLookup.apply("nonexistent")).thenReturn(null);

    // Act
    FieldDef result = docLookup.getFieldDef("nonexistent");

    // Assert
    assertNull(result);
    verify(mockFieldDefLookup).apply("nonexistent");
  }

  @Test
  public void testGetSegmentLookup() {
    // Arrange
    LeafReaderContext mockContext = mock(LeafReaderContext.class);

    // Act
    SegmentDocLookup result = docLookup.getSegmentLookup(mockContext);

    // Assert
    assertNotNull(result);
    // We can't test more without access to SegmentDocLookup internals
    // but we can verify it doesn't throw an exception
  }

  @Test
  public void testConstructor_NullFieldDefLookup() {
    // Act & Assert
    try {
      new DocLookup(null, mockAllFieldNamesSupplier);
      // Constructor doesn't validate null parameters, so this should not throw
    } catch (Exception e) {
      fail("Constructor should not throw for null fieldDefLookup: " + e.getMessage());
    }
  }

  @Test
  public void testConstructor_NullAllFieldNamesSupplier() {
    // Act & Assert
    try {
      new DocLookup(mockFieldDefLookup, null);
      // Constructor doesn't validate null parameters, so this should not throw
    } catch (Exception e) {
      fail("Constructor should not throw for null allFieldNamesSupplier: " + e.getMessage());
    }
  }
}
