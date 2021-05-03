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