package com.robaho.jleveldb;

import java.nio.ByteBuffer;

class CompressedKey {
    static byte[] decodeKey(int keylen,byte[] prevKey,ByteBuffer buffer){
        DecodedKeyLen dkyl= decodeKeyLen(keylen);
        byte[] key = new byte[dkyl.compressedLen+dkyl.prefixLen];
        buffer.get(key,0,dkyl.compressedLen);
        if (dkyl.prefixLen != 0) {
            System.arraycopy(key,0,key,dkyl.prefixLen,dkyl.compressedLen);
            System.arraycopy(prevKey,0,key,0,dkyl.prefixLen);
        }
        return key;
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
            return new DecodedKeyLen(0,keylen);
        }
    }
}
