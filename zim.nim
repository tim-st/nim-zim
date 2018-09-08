import strutils
import streams
import md5
import random
import lzma
import tables

# for details see: http://www.openzim.org/wiki/ZIM_file_format

const
  magicNumberZimFormat: int32 = 72173914
  noMainPage: int32 = int32.high
  noLayoutPage: int32 = int32.high

type ZimUuid* = array[16, uint8]

type DirectoryEntryKind* = enum
  ArticleEntry
  RedirectEntry
  LinkTarget
  DeletedEntry

type ZimMimetype = uint16

const
  DeletedEntryValue: ZimMimetype = 0xFFFD
  LinkTargetValue: ZimMimetype = 0xFFFE
  RedirectEntryValue: ZimMimetype = 0xFFFF

type ZimHeader* = object
  magicNumber: int32 # Magic number to recognise the file format, must be 72173914
  majorVersion: int16 # Major version of the ZIM file format (5 or 6)
  minorVersion: int16 # Minor version of the ZIM file format
  uuid: ZimUuid # unique id of this zim file
  articleCount: int32 # total number of articles
  clusterCount: int32 # total number of clusters
  urlPtrPos: int64 # position of the directory pointerlist ordered by URL
  titlePtrPos: int64 # position of the directory pointerlist ordered by Title
  clusterPtrPos: int64 # position of the cluster pointer list
  mimeListPos: int64 # position of the MIME type list (also header size)
  mainPage: int32 # main page or 0xffffffff if no main page
  layoutPage: int32 # layout page or 0xffffffff if no layout page
  checksumPos: int64 # pointer to the md5checksum of this file without the checksum itself. This points always 16 bytes before the end of the file.

type ZimFile* = object
  filename: string
  header: ZimHeader
  metadata*: Table[string, string]
  mimetypeList*: seq[string]
  stream: FileStream

proc len*(z: ZimFile): int = z.header.articleCount
proc filesize*(z: ZimFile): int = z.header.checksumPos.int + 16
proc uuid*(z: ZimFile): ZimUuid = z.header.uuid
proc close*(z: ZimFile) = z.stream.close()

proc readHeader(z: var ZimFile) =
  z.stream.setPosition(0)
  z.header.magicNumber = z.stream.readInt32
  z.header.majorVersion = z.stream.readInt16
  z.header.minorVersion = z.stream.readInt16
  doAssert z.stream.readData(addr(z.header.uuid[0]), sizeof(ZimUuid)) == sizeof(ZimUuid)
  z.header.articleCount = z.stream.readInt32
  z.header.clusterCount = z.stream.readInt32
  z.header.urlPtrPos = z.stream.readInt64
  z.header.titlePtrPos = z.stream.readInt64
  z.header.clusterPtrPos = z.stream.readInt64
  z.header.mimeListPos = z.stream.readInt64
  z.header.mainPage = z.stream.readInt32
  z.header.layoutPage = z.stream.readInt32
  z.header.checksumPos = z.stream.readInt64

proc readZeroTerminated(s: Stream): string =
  while true:
    let c = s.readChar
    if c == '\0': break
    result.add(c)

proc readMimeTypeList(z: var ZimFile) =
  z.mimetypeList = newSeqOfCap[string](32)
  z.stream.setPosition(z.header.mimeListPos.int)
  while true:
    let mimetype = z.stream.readZeroTerminated()
    if mimetype.len == 0: break
    z.mimetypeList.add(mimeType.toLowerAscii.strip)

const
  namespaceLayout* = '-'
  namespaceArticles* = 'A'
  namespaceArticleMetadata* = 'B'
  namespaceImagesFiles* = 'I'
  namespaceImagesText* = 'J'
  namespaceZimMetadata* = 'M'
  namespaceCategoriesText* = 'U'
  namespaceCategoriesArticleList* = 'V'
  namespaceCategoriesPerArticleCategoryList* = 'W'
  namespaceFulltextIndex* = 'X'

type DirectoryEntry* = object
  mimetype*: ZimMimetype # MIME type number as defined in the MIME type list
  parameterLen: byte # (not used) length of extra paramters
  namespace*: char # defines to which namespace this directory entry belongs
  revision*: int32 # (optional) identifies a revision of the contents of this directory entry, needed to identify updates or revisions in the original history
  case kind*: DirectoryEntryKind # MIME type number as defined in the MIME type list
  of ArticleEntry:
    clusterNumber*: int32 # cluster number in which the data of this directory entry is stored
    blobNumber*: int32 # blob number inside the compress cluster where the contents are stored
  of RedirectEntry:
    redirectIndex*: int32 # pointer to the directory entry of the redirect target
  of DeletedEntry, LinkTarget: discard # no extra fields
  url*: string # string with the URL as refered in the URL pointer list
  title*: string # string with an title as refered in the Title pointer list or empty; in case it is empty, the URL is used as title
  parameter*: string # (not used) extra parameters; see `parameterLen`

proc contentType*(z: ZimFile, entry: DirectoryEntry): string =
  if entry.mimetype.int in 0..<z.mimetypeList.len:
    result = z.mimetypeList[entry.mimetype.int]

proc isHtmlDocument*(z: ZimFile, entry: DirectoryEntry): bool =
  z.contentType(entry) == "text/html"

proc urlPointerAtPos(z: ZimFile, pos: Natural): int =
  z.stream.setPosition(z.header.urlPtrPos.int+pos*8)
  result = z.stream.readInt64.int

proc titlePointerAtPos(z: ZimFile, pos: Natural): int =
  z.stream.setPosition(z.header.titlePtrPos.int+pos*4)
  result = z.urlPointerAtPos(z.stream.readInt32.int)

proc clusterPointerAtPos(z: ZimFile, pos: Natural): int =
  z.stream.setPosition(z.header.clusterPtrPos.int+pos*8)
  result = z.stream.readInt64.int

proc readDirectoryEntry*(z: ZimFile, position: int, followRedirects = true): DirectoryEntry =
  z.stream.setPosition(position)
  result.mimetype = z.stream.readUint16
  result.parameterLen = z.stream.readUint8
  result.namespace = z.stream.readChar
  result.revision = z.stream.readInt32
  case result.mimetype
  of DeletedEntryValue:
    result.kind = DeletedEntry
  of LinkTargetValue:
    result.kind = LinkTarget
  of RedirectEntryValue:
    result.kind = RedirectEntry
    result.redirectIndex = z.stream.readInt32
    if followRedirects:
      return z.readDirectoryEntry(z.urlPointerAtPos(result.redirectIndex), true)
  else:
    result.kind = ArticleEntry
    result.clusterNumber = z.stream.readInt32
    result.blobNumber = z.stream.readInt32
  result.url = z.stream.readZeroTerminated
  result.title = z.stream.readZeroTerminated
  if unlikely(result.parameterLen.int > 0):
    result.parameter = z.stream.readStr(result.parameterLen.int)

proc isArticle*(entry: DirectoryEntry): bool =
  result = entry.kind == ArticleEntry and entry.namespace == namespaceArticles

proc isRedirect*(entry: DirectoryEntry): bool =
  result = entry.kind == RedirectEntry

proc followRedirect*(z: ZimFile, redirectIndex: int): DirectoryEntry =
  result = z.readDirectoryEntry(z.urlPointerAtPos(redirectIndex), true)

proc followRedirect*(z: ZimFile, entry: DirectoryEntry): DirectoryEntry =
  if likely(entry.isRedirect): z.followRedirect(entry.redirectIndex.int)
  else: entry

iterator entriesSortedByUrl*(z: ZimFile, reverse = false, limit = -1): DirectoryEntry =
  if reverse:
    let l = if limit > 0 and limit < z.len: z.len-1-limit else: 0
    for x in countdown(z.len-1, l):
      yield z.readDirectoryEntry(z.urlPointerAtPos(x), false)
  else:
    let l = if limit > 0: min(limit, z.len-1) else: z.len-1
    for x in countup(0, l):
      yield z.readDirectoryEntry(z.urlPointerAtPos(x), false)

iterator entriesSortedByNamespace*(z: ZimFile, namespace: char, limit = -1): DirectoryEntry =
  let reverse = namespace > namespaceArticles
  let cmpToInt = if reverse: -1 else: 1
  for entry in z.entriesSortedByUrl(reverse, limit):
    if cmp(entry.namespace, namespace) == cmpToInt: break
    if entry.namespace == namespace:
      yield entry

iterator entriesSortedByTitle*(z: ZimFile, limit = -1): DirectoryEntry =
  let l = if limit > 0: min(limit, z.len-1) else: z.len-1
  for x in 0..l:
    yield z.readDirectoryEntry(z.titlePointerAtPos(x), false)

proc linearSearchImpl(z: ZimFile, namespace: char, candidate: string, searchTitle: static[bool], limit = -1):
    tuple[entry: DirectoryEntry, success: bool] =
  ## Runtime: O(n)
  for entry in z.entriesSortedByNamespace(namespace, limit):
    result.entry = entry
    if (when searchTitle: result.entry.title else: result.entry.url) == candidate:
      result.success = true
      break

proc linearSearchByUrl*(z: ZimFile, url: string, namespace = namespaceArticles, limit = -1):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.linearSearchImpl(namespace, url, false, limit)

proc linearSearchByTitle*(z: ZimFile, title: string, namespace = namespaceArticles, limit = -1):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.linearSearchImpl(namespace, title, true, limit)

proc binarySearchImpl(z: ZimFile, namespace: char, candidate: string, searchTitle: static[bool]):
    tuple[entry: DirectoryEntry, success: bool] =
  ## Runtime: O(log_2(n))
  var firstUrlPosition = 0
  var lastUrlPosition = z.len - 1
  while firstUrlPosition <= lastUrlPosition:
    let middleUrlPosition = firstUrlPosition + ((lastUrlPosition - firstUrlPosition) div 2)
    result.entry = z.readDirectoryEntry(z.urlPointerAtPos(middleUrlPosition), followRedirects = false)
    var c = cmp(result.entry.namespace, namespace)
    if c == 0:
      c = cmp(when searchTitle: result.entry.title else: result.entry.url, candidate)
    if c == 0:
      result.success = true
      break
    if c == -1: firstUrlPosition = middleUrlPosition + 1
    else: lastUrlPosition = middleUrlPosition - 1

proc binarySearchByUrl*(z: ZimFile, url: string, namespace = namespaceArticles):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchImpl(namespace, url, false)

proc binarySearchByTitle*(z: ZimFile, title: string, namespace = namespaceArticles):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchImpl(namespace, title, true)

proc readDirectoryEntry*(z: ZimFile, url: string, namespace = namespaceArticles, followRedirects = true):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchByUrl(url, namespace)
  if result.success and followRedirects and result.entry.isRedirect:
    result.entry = z.readDirectoryEntry(z.urlPointerAtPos(result.entry.redirectIndex), true)

proc readDirectoryEntryByTitle*(z: ZimFile, title: string, namespace = namespaceArticles, followRedirects = true):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchByTitle(title, namespace)
  if result.success and followRedirects and result.entry.isRedirect:
    result.entry = z.readDirectoryEntry(z.urlPointerAtPos(result.entry.redirectIndex), true)

proc containsTitle*(z: ZimFile, title: string, namespace = namespaceArticles): bool =
  result = z.binarySearchByTitle(title, namespace).success

proc containsUrl*(z: ZimFile, url: string, namespace = namespaceArticles): bool =
  result = z.binarySearchByUrl(url, namespace).success

proc contains*(z: ZimFile, title: string, namespace = namespaceArticles): bool =
  result = containsTitle(z, title, namespace)

proc internalChecksum*(z: ZimFile): MD5Digest =
  z.stream.setPosition(z.header.checksumPos.int)
  doAssert z.stream.readData(addr(result[0]), 16) == 16

proc toMD5(s: Stream, length: Positive, blockSize: static[Positive]): MD5Digest =
  var
    context: MD5Context
    buffer = newString(blockSize)
  md5Init(context)
  var i = 0
  while i < length:
    let chunkSize = if i + blockSize < length: blockSize else: length - i
    if unlikely(chunkSize <= 0): break
    let bytesRead = s.readData(addr(buffer[0]), chunkSize)
    if unlikely(bytesRead != chunkSize): break
    md5Update(context, buffer, bytesRead)
    inc(i, bytesRead)
  md5Final(context, result)

proc calculatedChecksum*(z: ZimFile): MD5Digest =
  z.stream.setPosition(0)
  result = z.stream.toMD5(z.filesize-16, 8192*128)

proc matchesChecksum*(z: ZimFile): bool =
  z.internalChecksum == z.calculatedChecksum

proc readBlobAt*(z: ZimFile, clusterPosition, blobPosition: Natural): string =
  let thisClusterPointer = z.clusterPointerAtPos(clusterPosition)
  z.stream.setPosition(thisClusterPointer.int)
  let clusterInformation = z.stream.readUint8
  let isExtended = (clusterInformation and 0b0001_0000) == 0b0001_0000
  let offsetSize = if isExtended: 8 else: 4
  case clusterInformation and 0b0000_1111
  of 0, 1: # no compression; often used for images and files
    let thisBlobIndex = thisClusterPointer + 1 + blobPosition * offsetSize
    var thisBlobPointer: int
    var nextBlobPointer: int
    z.stream.setPosition(thisBlobIndex)
    if likely(offsetSize == 4):
      thisBlobPointer = z.stream.readInt32.int
      nextBlobPointer = z.stream.readInt32.int
    else:
      thisBlobPointer = z.stream.readInt64.int
      nextBlobPointer = z.stream.readInt64.int
    let blobLen = nextBlobPointer - thisBlobPointer
    z.stream.setPosition(thisClusterPointer + 1 + thisBlobPointer)
    result = z.stream.readStr(blobLen)
  of 4: # lzma compressed; often used for html and layout files
    var nextClusterPointer: int64
    if unlikely(clusterPosition == z.header.clusterCount-1):
      nextClusterPointer = z.header.checksumPos-1
    else:
      nextClusterPointer = z.clusterPointerAtPos(clusterPosition+1)
    let clusterLen = nextClusterPointer - thisClusterPointer - 1
    z.stream.setPosition(thisClusterPointer+1)
    var clusterData = lzma.decompress(z.stream.readStr(clusterLen.int))
    let thisBlobIndex = blobPosition * offsetSize
    let nextBlobIndex = thisBlobIndex + offsetSize
    var thisBlobPointer: int
    var nextBlobPointer: int
    if likely(offsetSize == 4):
      thisBlobPointer = int(cast[ptr int32](addr(clusterData[thisBlobIndex]))[])
      nextBlobPointer = int(cast[ptr int32](addr(clusterData[nextBlobIndex]))[])
    else:
      thisBlobPointer = int(cast[ptr int64](addr(clusterData[thisBlobIndex]))[])
      nextBlobPointer = int(cast[ptr int64](addr(clusterData[nextBlobIndex]))[])
    result = clusterData[thisBlobPointer..<nextBlobPointer]
  else: raise newException(ValueError, "Unsupported cluster compression: " & $clusterInformation)

proc readBlob*(z: ZimFile, entry: DirectoryEntry): string =
  assert entry.kind == ArticleEntry
  result = z.readBlobAt(entry.clusterNumber, entry.blobNumber)

proc hasMainPage*(z: ZimFile): bool = z.header.mainPage != noMainPage
proc hasLayoutPage*(z: ZimFile): bool = z.header.layoutPage != noLayoutPage
proc mainPage*(z: ZimFile): DirectoryEntry

proc randomArticleEntry*(z: ZimFile): DirectoryEntry =
  randomize()
  var tries = 0
  while tries != 10:
    let randomPosition = rand(z.len-1)
    result = z.readDirectoryEntry(z.urlPointerAtPos(randomPosition))
    if result.isArticle and result.title.len > 0: return
    inc(tries)
  if likely(z.hasMainPage): result = z.mainPage

proc mainPage*(z: ZimFile): DirectoryEntry =
  if likely(z.hasMainPage): z.followRedirect(z.header.mainPage.int)
  else: z.randomArticleEntry

proc layoutPage*(z: ZimFile): DirectoryEntry =
  if likely(z.hasLayoutPage): z.followRedirect(z.header.layoutPage.int)
  else: z.mainPage

proc readMetadata(z: var ZimFile) =
  z.metadata = initTable[string, string](16)
  for entry in z.entriesSortedByNamespace(namespaceZimMetadata):
    z.metadata[entry.url] = z.readBlob(entry)

proc getFavicon*(z: ZimFile): DirectoryEntry =
  let r = z.readDirectoryEntry("favicon", namespaceLayout, true)
  result = r.entry

proc getMetadata*(z: ZimFile, key: string): string = z.metadata.getOrDefault(key)

proc getName*(z: ZimFile): string = z.getMetadata("Name")
proc getTitle*(z: ZimFile): string = z.getMetadata("Title")
proc getCreator*(z: ZimFile): string = z.getMetadata("Creator")
proc getPublisher*(z: ZimFile): string = z.getMetadata("Publisher")
proc getDate*(z: ZimFile): string = z.getMetadata("Date")
proc getDescription*(z: ZimFile): string = z.getMetadata("Description")
proc getLongDescription*(z: ZimFile): string = z.getMetadata("LongDescription")
proc getLanguage*(z: ZimFile): string = z.getMetadata("Language")
proc getLicense*(z: ZimFile): string = z.getMetadata("License")
proc getTags*(z: ZimFile): string = z.getMetadata("Tags")
proc getRelation*(z: ZimFile): string = z.getMetadata("Relation")
proc getSource*(z: ZimFile): string = z.getMetadata("Source")
proc getCounter*(z: ZimFile): string = z.getMetadata("Counter")

proc newZimFileReader*(filename: string): ZimFile =
  result.filename = filename
  result.stream = newFileStream(filename, fmRead)
  result.readHeader()
  doAssert result.header.magicNumber == magicNumberZimFormat
  result.readMimeTypeList()
  result.readMetadata()

when isMainModule:
  import asynchttpserver, asyncdispatch, os, uri

  proc main() = 
    case paramCount()
    of 1: discard # TODO: support custom port
    else: raise newException(ValueError, "Usage: zim PathToZimFile")
    let zimFilename = paramStr(1).strip # https://download.kiwix.org/zim/
    var reader = newZimFileReader(zimFilename)
    var zimName = reader.getName.decodeUrl
    var zimNameLen = zimName.len
    var urlMainpage = reader.mainPage.url

    proc redirectTo(req: Request, namespace: char, url: string) {.async.} =
      let headers = newHttpHeaders(
        [
          ("Cache-Control", "max-age=87840, must-revalidate"),
          ("Location", '/' & zimName & '/' & namespace & '/' & url),
        ]
      )
      await req.respond(Http301, "", headers)

    proc redirectToMainpage(req: Request) {.async.} =
      let headers = newHttpHeaders(
        [
          ("Cache-Control", "no-store"),
          ("Location", '/' & zimName & '/' & namespaceArticles & '/' & urlMainpage),
        ]
      )
      await req.respond(Http301, "", headers)

    proc responseOk(req: Request, entry: DirectoryEntry) {.async.} =
      let blob = reader.readBlob(entry)
      let headers = newHttpHeaders(
        [
          ("Content-Type", reader.contentType(entry)),
          ("Cache-Control", "max-age=87840, must-revalidate"),
          ("Connection", "Close")
        ]
      )
      await req.respond(Http200, blob, headers)
    
    var server = newAsyncHttpServer(maxBody = 0)
    proc handleRequest(req: Request) {.async.} =
      let path = req.url.path
      when not defined(release):
        echo path
      var decodedPath: string
      try: decodedPath = decodeUrl(path) # FIXME: path = "/%"
      except: decodedPath = path
      if unlikely(decodedPath == "/favicon.ico"):
        await req.responseOk(reader.getFavicon)
      elif unlikely(
          decodedPath.len < zimNameLen + 5 or
          not decodedPath.startsWith('/' & zimName & '/') or
          decodedPath[zimNameLen+3] != '/' or
          decodedPath[zimNameLen+2] notin {
            namespaceLayout,
            namespaceArticles,
            namespaceImagesFiles
          }): await req.redirectToMainpage
      else:
        let namespace = decodedPath[zimNameLen+2]
        var url = decodedPath[zimNameLen + 4..^1]
        let r = reader.readDirectoryEntry(url, namespace)
        if likely(r.success):
          await req.responseOk(r.entry)
        elif namespace != namespaceArticles:
          await req.redirectToMainpage
        else:
          # The user looked for an article but the filename was not found:
          # We search for the best match and redirect because the filename is definetly different
          # to the filename that was requested.
          if not url.endsWith(".html"): url = url & ".html" # gives better results
          let bestMatchResult = reader.readDirectoryEntry(url, namespaceArticles, true)
          await req.redirectTo(namespace, bestMatchResult.entry.url)

    echo "Serving ZIM file at http://127.0.0.1:8080" & '/' & zimName & '/' & namespaceArticles & '/' & urlMainpage
    echo reader.getTitle
    echo reader.getDescription
    echo reader.getDate
    echo "Press CTRL-C to stop the server."
    waitFor server.serve(Port(8080), handleRequest)
  
  main()