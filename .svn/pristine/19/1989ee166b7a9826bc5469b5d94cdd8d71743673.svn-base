# 错误码设计

这是ScanEngine的错误码设计文档，有两部分数字拼接而成。
protocol_code + error_code。
其中:
1. protocol_code为协议码，是协议标准rfc的编号，参考https://www.rfc-editor.org/。 
2. error_code为错误码, 如果协议中有状态码直接复用，没有状态码则自己定义，使用os错误码。

## 错误码表
|协议   |协议码 |错误码 |完整错误码 |错误描述   |
|-------|------|------|---------|----------|
|SFTP   |4252  |0051  |42520051  |AuthenticationException 认证错误|
|SFTP   |4253  |0002  |42530002  |SSHException 协议错误|
|SFTP   |4253  |0008  |42530008  |IncompatiblePeer 协议版本不兼容|
|SFTP   |4253  |0009  |42530009  |BadHostKeyException HostKey错误|
|SFTP   |4254  |0001  |42540001  |ChannelException SSH_OPEN_ADMINISTRATIVELY_PROHIBITED 策略禁止访问|
|SFTP   |4254  |0002  |42540002  |ChannelException SSH_OPEN_CONNECT_FAILED 连接通道失败|
|SFTP   |4254  |0003  |42540003  |ChannelException SSH_OPEN_UNKNOWN_CHANNEL_TYPE 未知的通道类型|
|SFTP   |4254  |0004  |42540004  |ChannelException SSH_OPEN_RESOURCE_SHORTAGE 资源限制|
|SFTP   |4254  |0051  |42540051  |CouldNotCanonicalize 规范化输入误|
|SMB    |1094  |0001  |10940001  |NotReadyError, NotConnectedError 认证失败|
|SMB    |1094  |0002  |10940002  |ProtocolError 协议错误|
|SMB    |1094  |0003  |10940003  |OperationFailure 操作失败|
|S3     |127   |0000  |12700000  |ClientError|
|S3     |127   |1301  |12701301  |PermanentRedirect|
|S3     |127   |2301  |12702301  |PermanentRedirectControlError|
|S3     |127   |1304  |12701304  |NotModified|
|S3     |127   |1307  |12701307  |Redirect|
|S3     |127   |2307  |12702307  |TemporaryRedirect|
|S3     |127   |1400  |12701400  |AccessControlListNotSupported|
|S3     |127   |2400  |12702400  |AmbiguousGrantByEmailAddress|
|S3     |127   |3400  |12703400  |AuthorizationHeaderMalformed|
|S3     |127   |4400  |12704400  |AuthorizationQueryParametersError|
|S3     |127   |5400  |12705400  |BadDigest|
|S3     |127   |6400  |12706400  |BucketHasAccessPointsAttached|
|S3     |127   |7400  |12707400  |ConnectionClosedByRequester|
|S3     |127   |8400  |12708400  |CredentialsNotSupported|
|S3     |127   |9400  |12709400  |DeviceNotActiveError|
|S3     |127   |10400 |12710400  |EndpointNotFound|
|S3     |127   |11400 |12711400  |EntityTooSmall|
|S3     |127   |12400 |12712400  |EntityTooLarge|
|S3     |127   |13400 |12713400  |ExpiredToken|
|S3     |127   |14400 |12714400  |IllegalLocationConstraintException|
|S3     |127   |15400 |12715400  |IllegalVersioningConfigurationException|
|S3     |127   |16400 |12716400  |IncompleteBody|
|S3     |127   |17400 |12717400  |IncorrectEndpoint|
|S3     |127   |18400 |12718400  |IncorrectNumberOfFilesInPostRequest|
|S3     |127   |19400 |12719400  |InlineDataTooLarge|
|S3     |127   |20400 |12720400  |InvalidAccessPoint|
|S3     |127   |21400 |12721400  |InvalidAccessPointAliasError|
|S3     |127   |22400 |12722400  |InvalidArgument|
|S3     |127   |23400 |12723400  |InvalidBucketAclWithObjectOwnership|
|S3     |127   |24400 |12724400  |InvalidBucketName|
|S3     |127   |25400 |12725400  |InvalidBucketOwnerAWSAccountID|
|S3     |127   |26400 |12726400  |InvalidDigest|
|S3     |127   |27400 |12727400  |InvalidEncryptionAlgorithmError|
|S3     |127   |28400 |12728400  |InvalidHostHeader|
|S3     |127   |29400 |12729400  |InvalidHttpMethod|
|S3     |127   |30400 |12730400  |InvalidLocationConstraint|
|S3     |127   |31400 |12731400  |InvalidPart|
|S3     |127   |32400 |12732400  |InvalidPartOrder|
|S3     |127   |33400 |12733400  |InvalidPolicyDocument|
|S3     |127   |34400 |12734400  |InvalidRequest|
|S3     |127   |35400 |12735400  |InvalidSessionException|
|S3     |127   |36400 |12736400  |InvalidSignature|
|S3     |127   |37400 |12737400  |InvalidSOAPRequest|
|S3     |127   |38400 |12738400  |InvalidStorageClass|
|S3     |127   |39400 |12739400  |InvalidTargetBucketForLogging|
|S3     |127   |40400 |12740400  |InvalidToken|
|S3     |127   |41400 |12741400  |InvalidURI|
|S3     |127   |42400 |12742400  |KeyTooLongError|
|S3     |127   |43400 |12743400  |KMS.DisabledException|
|S3     |127   |44400 |12744400  |KMS.InvalidKeyUsageException|
|S3     |127   |45400 |12745400  |KMS.KMSInvalidStateException|
|S3     |127   |46400 |12746400  |KMS.NotFoundException|
|S3     |127   |47400 |12747400  |MalformedACLError|
|S3     |127   |48400 |12748400  |MalformedPOSTRequest|
|S3     |127   |49400 |12749400  |MalformedXML|
|S3     |127   |50400 |12750400  |MaxMessageLengthExceeded|
|S3     |127   |51400 |12751400  |MaxPostPreDataLengthExceededError|
|S3     |127   |52400 |12752400  |MetadataTooLarge|
|S3     |127   |53400 |12753400  |MissingAttachment|
|S3     |127   |54400 |12754400  |MissingRequestBodyError|
|S3     |127   |55400 |12755400  |MissingSecurityElement|
|S3     |127   |56400 |12756400  |MissingSecurityHeader|
|S3     |127   |57400 |12757400  |NoLoggingStatusForKey|
|S3     |127   |58400 |12758400  |NoSuchAsyncRequest|
|S3     |127   |59400 |12759400  |NotDeviceOwnerError|
|S3     |127   |60400 |12760400  |RequestHeaderSectionTooLarge|
|S3     |127   |61400 |12761400  |RequestTimeout|
|S3     |127   |62400 |12762400  |RequestTorrentOfBucketError|
|S3     |127   |63400 |12763400  |ResponseInterrupted|
|S3     |127   |64400 |12764400  |ServerSideEncryptionConfigurationNotFoundError|
|S3     |127   |65400 |12765400  |TagPolicyException|
|S3     |127   |66400 |12766400  |TokenCodeInvalidError|
|S3     |127   |67400 |12767400  |TokenRefreshRequired|
|S3     |127   |68400 |12768400  |TooManyAccessPoints|
|S3     |127   |69400 |12769400  |TooManyBuckets|
|S3     |127   |70400 |12770400  |TooManyMultiRegionAccessPointregionsError|
|S3     |127   |71400 |12771400  |TooManyMultiRegionAccessPoints|
|S3     |127   |72400 |12772400  |UnexpectedContent|
|S3     |127   |73400 |12773400  |UnsupportedArgument|
|S3     |127   |74400 |12774400  |UnsupportedSignature|
|S3     |127   |75400 |12775400  |UnresolvableGrantByEmailAddress|
|S3     |127   |76400 |12776400  |UserKeyMustBeSpecified|
|S3     |127   |77400 |12777400  |InvalidTag|
|S3     |127   |78400 |12778400  |MalformedPolicy|
|S3     |127   |1403  |12701403  |AccessDenied|
|S3     |127   |2403  |12702403  |AccountProblem|
|S3     |127   |3403  |12703403  |AllAccessDisabled|
|S3     |127   |4403  |12704403  |CrossLocationLoggingProhibited|
|S3     |127   |5403  |12705403  |InvalidAccessKeyId|
|S3     |127   |6403  |12706403  |InvalidObjectState|
|S3     |127   |7403  |12707403  |InvalidPayer|
|S3     |127   |8403  |12708403  |InvalidRegion|
|S3     |127   |9403  |12709403  |InvalidSecurity|
|S3     |127   |10403 |12710403  |MissingAuthenticationToken|
|S3     |127   |11403 |12711403  |NotSignedUp|
|S3     |127   |12403 |12712403  |RequestTimeTooSkewed|
|S3     |127   |13403 |12713403  |SignatureDoesNotMatch|
|S3     |127   |14403 |12714403  |UnauthorizedAccessError|
|S3     |127   |15403 |12715403  |UnexpectedIPError|
|S3     |127   |1404  |12701404  |NoSuchAsyncRequest|
|S3     |127   |2404  |12702404  |NoSuchBucket|
|S3     |127   |3404  |12703404  |NoSuchBucketPolicy|
|S3     |127   |4404  |12704404  |NoSuchCORSConfiguration|
|S3     |127   |5404  |12705404  |NoSuchKey|
|S3     |127   |6404  |12706404  |NoSuchLifecycleConfiguration|
|S3     |127   |7404  |12707404  |NoSuchMultiRegionAccessPoint|
|S3     |127   |8404  |12708404  |NoSuchObjectLockConfiguration|
|S3     |127   |9404  |12709404  |NoSuchWebsiteConfiguration|
|S3     |127   |10404 |12710404  |NoSuchTagSet|
|S3     |127   |11404 |12711404  |NoSuchUpload|
|S3     |127   |12404 |12712404  |NoSuchVersion|
|S3     |127   |13404 |12713404  |NoTransformationDefined|
|S3     |127   |14404 |12714404  |ObjectLockConfigurationNotFoundError|
|S3     |127   |15404 |12715404  |OwnershipControlsNotFoundError|
|S3     |127   |16404 |12716404  |NoSuchAccessPoint|
|S3     |127   |1405  |12701405  |MethodNotAllowed|
|S3     |127   |1409  |12701409  |AccessPointAlreadyOwnedByYou|
|S3     |127   |2409  |12702409  |BucketAlreadyExists|
|S3     |127   |3409  |12703409  |BucketAlreadyOwnedByYou|
|S3     |127   |4409  |12704409  |BucketNotEmpty|
|S3     |127   |5409  |12705409  |ClientTokenConflict|
|S3     |127   |6409  |12706409  |ConditionalRequestConflict|
|S3     |127   |7409  |12707409  |InvalidBucketState|
|S3     |127   |8409  |12708409  |OperationAborted|
|S3     |127   |9409  |12709409  |RestoreAlreadyInProgress|
|S3     |127   |1411  |12701411  |MissingContentLength|
|S3     |127   |1412  |12701412  |PreconditionFailed|
|S3     |127   |2412  |12702412  |RequestIsNotMultiPartContent|
|S3     |127   |1416  |12701416  |InvalidRange|
|S3     |127   |1500  |12701500  |InternalError|
|S3     |127   |1501  |12701501  |NotImplemented|
|S3     |127   |1503  |12701503  |ServiceUnavailable|
|S3     |127   |2503  |12702503  |SlowDown|
|S3     |127   |3503  |12703503  |503 SlowDown|
|ftp    |959   |1300  |95901300  |error_reply|
|ftp    |959   |1400  |95901400  |error_temp|
|ftp    |959   |0421  |95900421  |error_temp Service not available|
|ftp    |959   |0425  |95900425  |error_temp Can't open data connection.|
|ftp    |959   |0426  |95900426  |error_temp Connection closed; transfer aborted. |
|ftp    |959   |0450  |95900450  |error_temp File unavailable|
|ftp    |959   |0451  |95900451  |error_temp Requested action aborted: local error in processing.|
|ftp    |959   |0452  |95900452  |error_temp Insufficient storage space in system.|
|ftp    |959   |1500  |95901500  |error_perm|
|ftp    |959   |0500  |95900500  |error_perm Syntax error, command unrecognized.|
|ftp    |959   |0501  |95900501  |error_perm Syntax error in parameters or arguments.|
|ftp    |959   |0502  |95900502  |error_perm Command not implemented.|
|ftp    |959   |0503  |95900503  |error_perm Bad sequence of commands.|
|ftp    |959   |0504  |95900504  |error_perm Command not implemented for that parameter.|
|ftp    |959   |0530  |95900530  |error_perm Not logged in.|
|ftp    |959   |0532  |95900532  |error_perm Need account for storing files.|
|ftp    |959   |0550  |95900550  |error_perm File unavailable (e.g., file not found, no access).|
|ftp    |959   |0551  |95900551  |error_perm Requested action aborted: page type unknown.|
|ftp    |959   |0552  |95900552  |error_perm Requested file action aborted. Exceeded storage allocation (for current directory ordataset).|
|ftp    |959   |0553  |95900553  |error_perm Requested action not taken. File name not allowed.|
|ftp    |959   |1600  |95901600  |error_proto|







## 备注说明
1. 对于SMB协议没有协议标准号，使用废弃的1094代替，三方库没有精细化区分错误，
其中ProtocolError有WinPlatform的错误码，具体请参照
https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-erref/1bc92ddf-b79e-413c-bbaa-99a5281a6c90

2. 对于协议无关的错误对应OSError，统一使用linux操作系统的错误码，具体请参照
https://www.man7.org/linux/man-pages/man3/errno.3.html#:~:text=The%20%3Cerrno.h%3E%20header%20file%20defines%20the%20integer%20variable,function%20that%20succeeds%20is%20allowed%20to%20change%20errno.
在Linux中可以使用 errno命令查看枚举需要和含义:
```bash
errno 13
>> EACCES 13 Permission denied
errno EACCES
>> EACCES 13 Permission denied
```

3. 对于S3协议没有协议标准号，使用废弃的127代替, 错误的具体描述请参照
https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html