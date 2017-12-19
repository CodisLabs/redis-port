#define END_OF_MACRO2(x, y) \
  typedef struct {          \
    void *p;                \
  } x##y

#define END_OF_MACRO(x, y) END_OF_MACRO2(x, y)

#define DISABLE_FUNCTION(func)                                                \
  void func(void) {                                                           \
    _serverPanic(__FILE__, __LINE__, "Function '%s' is disabled.", __func__); \
  }                                                                           \
  END_OF_MACRO(__struct__, __LINE__)

extern void _serverPanic(const char *file, int line, const char *vformat, ...);

DISABLE_FUNCTION(addDeferredMultiBulkLength);
DISABLE_FUNCTION(addReply);
DISABLE_FUNCTION(addReplyBulk);
DISABLE_FUNCTION(addReplyBulkCBuffer);
DISABLE_FUNCTION(addReplyBulkCString);
DISABLE_FUNCTION(addReplyBulkLongLong);
DISABLE_FUNCTION(addReplyBulkSds);
DISABLE_FUNCTION(addReplyDouble);
DISABLE_FUNCTION(addReplyError);
DISABLE_FUNCTION(addReplyErrorFormat);
DISABLE_FUNCTION(addReplyHelp);
DISABLE_FUNCTION(addReplyLongLong);
DISABLE_FUNCTION(addReplyMultiBulkLen);
DISABLE_FUNCTION(addReplySds);
DISABLE_FUNCTION(addReplyStatus);
DISABLE_FUNCTION(adjustOpenFilesLimit);
DISABLE_FUNCTION(aeCreateFileEvent);
DISABLE_FUNCTION(aeDeleteFileEvent);
DISABLE_FUNCTION(aeGetSetSize);
DISABLE_FUNCTION(aeResizeSetSize);
DISABLE_FUNCTION(aeWait);
DISABLE_FUNCTION(alsoPropagate);
DISABLE_FUNCTION(anetBlock);
DISABLE_FUNCTION(anetEnableTcpNoDelay);
DISABLE_FUNCTION(anetNonBlock);
DISABLE_FUNCTION(anetPeerToString);
DISABLE_FUNCTION(anetSendTimeout);
DISABLE_FUNCTION(anetSockName);
DISABLE_FUNCTION(anetTcpAccept);
DISABLE_FUNCTION(anetTcpNonBlockBindConnect);
DISABLE_FUNCTION(anetTcpNonBlockConnect);
DISABLE_FUNCTION(aofReadDiffFromParent);
DISABLE_FUNCTION(aofRewriteBufferSize);
DISABLE_FUNCTION(bioPendingJobsOfType);
DISABLE_FUNCTION(blockForKeys);
DISABLE_FUNCTION(clientsArePaused);
DISABLE_FUNCTION(closeChildInfoPipe);
DISABLE_FUNCTION(closeListeningSockets);
DISABLE_FUNCTION(countKeysInSlot);
DISABLE_FUNCTION(crc16);
DISABLE_FUNCTION(dbAdd);
DISABLE_FUNCTION(dbAsyncDelete);
DISABLE_FUNCTION(dbDelete);
DISABLE_FUNCTION(dbOverwrite);
DISABLE_FUNCTION(dbSyncDelete);
DISABLE_FUNCTION(dbUnshareStringValue);
DISABLE_FUNCTION(delKeysInSlot);
DISABLE_FUNCTION(disableWatchdog);
DISABLE_FUNCTION(emptyDb);
DISABLE_FUNCTION(enableWatchdog);
DISABLE_FUNCTION(evalCommand);
DISABLE_FUNCTION(evalShaCommand);
DISABLE_FUNCTION(execCommand);
DISABLE_FUNCTION(exitFromChild);
DISABLE_FUNCTION(flushSlavesOutputBuffers);
DISABLE_FUNCTION(freeClient);
DISABLE_FUNCTION(getClientOutputBufferMemoryUsage);
DISABLE_FUNCTION(getClientTypeByName);
DISABLE_FUNCTION(getClientTypeName);
DISABLE_FUNCTION(getExpire);
DISABLE_FUNCTION(getKeysFreeResult);
DISABLE_FUNCTION(getKeysFromCommand);
DISABLE_FUNCTION(getKeysInSlot);
DISABLE_FUNCTION(getPsyncInitialOffset);
DISABLE_FUNCTION(getTimeoutFromObjectOrReply);
DISABLE_FUNCTION(keyspaceEventsFlagsToString);
DISABLE_FUNCTION(keyspaceEventsStringToFlags);
DISABLE_FUNCTION(latencyAddSample);
DISABLE_FUNCTION(listenToPort);
DISABLE_FUNCTION(lookupCommand);
DISABLE_FUNCTION(lookupKeyRead);
DISABLE_FUNCTION(lookupKeyReadOrReply);
DISABLE_FUNCTION(lookupKeyWrite);
DISABLE_FUNCTION(lookupKeyWriteOrReply);
DISABLE_FUNCTION(luaCreateFunction);
DISABLE_FUNCTION(moduleFreeContext);
DISABLE_FUNCTION(moduleTypeLookupModuleByID);
DISABLE_FUNCTION(moduleTypeNameByID);
DISABLE_FUNCTION(notifyKeyspaceEvent);
DISABLE_FUNCTION(openChildInfoPipe);
DISABLE_FUNCTION(parseScanCursorOrReply);
DISABLE_FUNCTION(pauseClients);
DISABLE_FUNCTION(preventCommandPropagation);
DISABLE_FUNCTION(processEventsWhileBlocked);
DISABLE_FUNCTION(propagate);
DISABLE_FUNCTION(propagateExpire);
DISABLE_FUNCTION(pubsubPublishMessage);
DISABLE_FUNCTION(redisSetProcTitle);
DISABLE_FUNCTION(redis_check_rdb_main);
DISABLE_FUNCTION(refreshGoodSlavesCount);
DISABLE_FUNCTION(replaceClientCommandVector);
DISABLE_FUNCTION(replicationGetSlaveName);
DISABLE_FUNCTION(replicationGetSlaveOffset);
DISABLE_FUNCTION(replicationSendNewlineToMaster);
DISABLE_FUNCTION(replicationSetMaster);
DISABLE_FUNCTION(replicationSetupSlaveForFullResync);
DISABLE_FUNCTION(replicationUnsetMaster);
DISABLE_FUNCTION(resetCommandTableStats);
DISABLE_FUNCTION(resetServerStats);
DISABLE_FUNCTION(resizeReplicationBacklog);
DISABLE_FUNCTION(rewriteClientCommandArgument);
DISABLE_FUNCTION(rewriteClientCommandVector);
DISABLE_FUNCTION(rewriteConfigSentinelOption);
DISABLE_FUNCTION(scanGenericCommand);
DISABLE_FUNCTION(sendChildInfo);
DISABLE_FUNCTION(sentinelHandleConfiguration);
DISABLE_FUNCTION(serverLogHexDump);
DISABLE_FUNCTION(setDeferredMultiBulkLength);
DISABLE_FUNCTION(setExpire);
DISABLE_FUNCTION(setKey);
DISABLE_FUNCTION(signalKeyAsReady);
DISABLE_FUNCTION(signalModifiedKey);
DISABLE_FUNCTION(startAppendOnly);
DISABLE_FUNCTION(stopAppendOnly);
DISABLE_FUNCTION(syncReadLine);
DISABLE_FUNCTION(syncWrite);
DISABLE_FUNCTION(updateCachedTime);
DISABLE_FUNCTION(updateDictResizePolicy);
DISABLE_FUNCTION(updateSlavesWaitingBgsave);

#define DECLARE_FUNCTION(func)           \
  void *func(void) { return (void *)0; } \
  END_OF_MACRO(__struct__, __LINE__)

DECLARE_FUNCTION(changeReplicationId);
DECLARE_FUNCTION(clearReplicationId2);
DECLARE_FUNCTION(populateCommandTable);
DECLARE_FUNCTION(lookupCommandByCString);
