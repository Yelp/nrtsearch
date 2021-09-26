/*
 * Copyright 2021 Yelp Inc.
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
package com.chase.app.search;

public class TokenData {
  public TokenData(String text, int startOffset, int endOffset, int position) {
    this.Text = text;
    this.StartOffset = startOffset;
    this.EndOffset = endOffset;
    this.Position = position;
  }

  public String Text;
  public int StartOffset;
  public int EndOffset;
  public int Position;

  public int GetPosition() {
    return Position;
  }
}
