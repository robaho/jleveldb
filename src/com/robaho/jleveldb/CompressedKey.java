package com.robaho.jleveldb;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

class CompressedKey {
    static void decodeKey(KeyBuffer dst, KeyBuffer prevKey, int keylen, InputStream is) throws IOException {
        if((keylen & Constants.compressedBit)!=0) {
            int prefixLen = ((keylen >> 8) & 0xFFFF & Constants.maxPrefixLen);
            int compressedLen = (keylen & Constants.maxCompressedLen);
            dst.from(is,compressedLen);
            dst.insertPrefix(prevKey,prefixLen);
        } else {
            dst.from(is,keylen);
        }
    }

    private static class DecodedKeyLen {
        final int prefixLen,compressedLen;
        DecodedKeyLen(int prefixLen,int compressedLen){
            this.prefixLen = prefixLen;
            this.compressedLen = compressedLen;
        }
    }

    private static DecodedKeyLen decodeKeyLen(int keylen) {

        if ((keylen & Constants.compressedBit) != 0) {
            return new DecodedKeyLen(
                    ((keylen >> 8) & 0xFFFF & Constants.maxPrefixLen) ,
                    (keylen & Constants.maxCompressedLen));
        } else {
            if (keylen > Constants.maxKeySize) {
                throw new IllegalStateException("key > 1024");
            }
            if (keylen <= 0) {
                throw new IllegalStateException("key <= 0");
            }
            return null;
        }
    }
}
