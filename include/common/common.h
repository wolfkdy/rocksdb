//  Copyright (c) 2017-present.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_COMMON_H_
#define STORAGE_ROCKSDB_INCLUDE_COMMON_H_

#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif /* __cplusplus */

void*  __IndexMemAllocZero(uint32_t uiBytes);

void  __IndexMemFree(void *pvUsrMem);

#include "securec.h"

// The wrapper function for the memory application  about Secure_C_V100R001C01SPC004
#define CommonMemCopy(p_dst, dst_max, p_src, size)          memcpy_s((p_dst), (dst_max), (p_src), (size))
#define CommonMemZero(p_dst, size)                          memset_s((p_dst), (size), 0, (size))
#define CommonMemSet(p_dst, dst_max,byte_value, size)       memset_s((p_dst), (dst_max), (byte_value), (size))
#define CommonMemCmp(ptr1, ptr2, size)                      memcmp((ptr1), (ptr2), (size))
#define CommonStrCopy(p_dst, size, p_src)                   strcpy_s((p_dst), (size), (p_src))
#define CommonSnprintf(p_dst,dst_max, size, format...)      snprintf_s((p_dst), (dst_max), (size), ##format)
#define CommonVsnprintf(p_dst,dst_max, size, format, p_src)  vsnprintf_s((p_dst), (dst_max), (size), (format), (p_src))
#define CommonStrnCpy(p_dst,dst_max, p_src, size)           strncpy_s((p_dst), (dst_max), (p_src), (size))
#define CommonStrTok(p_src, delimit, next_src)              strtok_s((p_src), (delimit), (next_src))
#define CommonSscanf(p_dst, format...)                      sscanf_s(p_dst, ##format)
#define CommonFscanf(p_dst, format...)                      fscanf_s(p_dst, ##format)
#define CommonSprintf(p_dst, dst_max, format...)            sprintf_s((p_dst), (dst_max), ##format)
#define CommonMemMove(p_dst, dst_max, p_src, count)         memmove_s((p_dst), (dst_max), (p_src), (count))
#define CommonVsprintf(p_dst, dst_max, p_src, format...)    vsprintf_s((p_dst), (dst_max), (p_src), ##format)

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

#endif

