package com.robaho.jleveldb;

import java.nio.ByteBuffer;

class CompressedKey {
    static void decodeKey(KeyBuffer dst,KeyBuffer prevKey,int keylen,ByteBuffer buffer){
        DecodedKeyLen dkyl = decodeKeyLen(keylen);
        dst.from(buffer,dkyl==null ? keylen : dkyl.compressedLen);
        if (dkyl!=null) {
            dst.insertPrefix(prevKey,dkyl.prefixLen);
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
