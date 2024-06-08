package com.robaho.jleveldb;

import java.io.IOException;
import java.io.InputStream;

class CompressedKey {
    /*
     * dst must hold the previous key
     */
    static void decodeKey(KeyBuffer dst, int keylen, InputStream is) throws IOException {
        if((keylen & Constants.compressedBit)!=0) {
            int prefixLen = ((keylen >> 8) & 0xFFFF & Constants.maxPrefixLen);
            int compressedLen = (keylen & Constants.maxCompressedLen);
            dst.from(is,compressedLen,prefixLen);
        } else {
            dst.from(is,keylen,0);
        }
    }
}
