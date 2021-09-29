{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE ViewPatterns        #-}

-- | Read the dump generated my the trace calls during planning. This
-- will separate the branches into different files so they can be
-- examined individually. Usage:
--
-- mkdir -p /tmp/branches
-- stack run benchmark -- 20 & /tmp/benchmark.out
-- stack ghc -- -O2 Main.hs -o /tmp/readdump && /tmp/readdump > /dev/null

module Main where

import           Control.Monad
import           Control.Monad.State
import           Data.Bifunctor
import qualified Data.ByteString.Lazy       as B
import qualified Data.ByteString.Lazy.Char8 as C8
import           Data.Char
import           Data.List
import           Data.Maybe
import           GHC.Stack
import           System.Directory
import           System.Environment
import           Text.Printf
import           Text.Read                  (readMaybe)

type StringType = B.ByteString

type Linum = Int
data Node = NewPlan Int | Node Linum [Bool] StringType deriving Show

show' :: Show a => a -> StringType
show' = C8.pack . show
read' :: (HasCallStack,Read a) => StringType -> a
read' (dropWhile isSpace . C8.unpack -> x) =
  fromMaybe (error $ printf "Unreadable: '%s'" x) $ readMaybe x

isNewPlan :: Node -> Bool
isNewPlan (NewPlan _) = True
isNewPlan _           = False

pref :: StringType
pref = "[Before] Solving node:<"
lineToNode :: Linum -> StringType -> Maybe Node
lineToNode linum x
  | pref `B.isPrefixOf` x =
    Just
    $ NewPlan
    $ readWithin pref ">" x
  | "[branch:" `B.isPrefixOf` x =
      Just
      $ uncurry (Node linum)
      $ readId "oops" `bimap` C8.drop 2
      $ partitionParen
      $ B.drop (B.length "[branch:") x
  | otherwise = Nothing

intAfter :: StringType -> StringType -> Maybe Int
intAfter pre str = case dropPref pre str of
  Right _ -> Nothing
  Left x  -> Just $ read' $ C8.takeWhile isDigit x

readWithin :: StringType -> StringType -> StringType -> Int
readWithin pre suf x =
  read' $ B.drop (B.length pre) $ B.take (B.length x - B.length suf) x


dropPref :: StringType -> StringType -> Either StringType StringType
dropPref pre str = if C8.isPrefixOf pre str
  then Left $ C8.drop (C8.length pre) str
  else Right str

-- | Read the identity (sequence of bools) of a node.
readId :: String -> StringType -> [Bool]
#if 0
-- NOTE: Looks like GHC is pretty good at pattern matching strings and
-- the string version is faster here.
readId errMsg = either
  (const [])
  (readLst . fromLeft . dropPref "[")
  . dropPref "[]"
  where
    readLst :: StringType -> [Bool]
    readLst str = fromLeft
      $ case' True "EitherRight"
      =<< case' True "SplitRight"
      =<< case' True "AlsoRight"
      =<< case' False "EitherLeft"
      =<< case' False "SplitLeft"
      =<< case' False "AlsoLeft" str
    fromLeft :: Either a b -> a
    fromLeft = \case
      Left x  -> x
      Right _ -> error errMsg
    case' :: Bool -> StringType -> StringType -> Either [Bool] StringType
    case' val pat =
      first (either
             (const [val])
             ((val:) . readLst . fromLeft . dropPref ",")
             . dropPref "]")
      . dropPref pat
#else
readId errMsg = readStart . C8.unpack
  where
    readStart = \case
      "[]"     -> []
      '[':rest -> readProv rest
      _        -> error errMsg
    readProv = \case
      'E':'i':'t':'h':'e':'r':'L':'e':'f':'t':rest     -> False:readCom rest
      'E':'i':'t':'h':'e':'r':'R':'i':'g':'h':'t':rest -> True:readCom rest
      'S':'p':'l':'i':'t':'L':'e':'f':'t':rest         -> False:readCom rest
      'S':'p':'l':'i':'t':'R':'i':'g':'h':'t':rest     -> True:readCom rest
      'A':'l':'s':'o':'R':'i':'g':'h':'t':rest         -> True:readCom rest
      'A':'l':'s':'o':'L':'e':'f':'t':rest             -> False:readCom rest
      _                                                -> error errMsg
    readCom = \case
      ',':rest -> readProv rest
      "]"      -> []
      _        -> error errMsg
#endif

data ProvenanceAtom
  = EitherLeft
  | EitherRight
  | SplitLeft
  | SplitRight
  | AlsoLeft
  | AlsoRight
  deriving (Show,Read)
provenanceAsBool :: ProvenanceAtom -> Bool
provenanceAsBool = \case
  EitherLeft  -> False
  SplitLeft   -> False
  AlsoLeft    -> False
  EitherRight -> True
  SplitRight  -> True
  AlsoRight   -> True
partitionParen :: StringType -> (StringType, StringType)
partitionParen str = C8.splitAt (i + 1) str where
  i = snd
    $ head
    $ dropWhile ((> 0) . fst)
    $ tail
    $ scanl (\(x,_) (y,i') -> (x+y,i')) (0,0)
    $ sortOn snd
    $ opens ++ closes
  opens = (1 :: Int,) <$> C8.findIndices (== '[') str
  closes = (-1,) <$> C8.findIndices (== ']') str

splitWhen :: (a -> Maybe b) -> [a] -> ([a], [(b, [a])])
splitWhen f as = (fst <$> heading, go restPairs) where
  pairs = zip as $ fmap f as
  (heading, restPairs) = splitWhen' (isJust . snd) pairs
  go :: [(a, Maybe b)] -> [(b, [a])]
  go [] = []
  go ((_,Just b):xs) = (b, fst <$> firstTail):go rest where
    (firstTail, rest) = splitWhen' (isJust . snd) xs
  go _ = undefined
  splitWhen' f' xs = splitAt (length $ takeWhile (not . f') xs) xs

data NodeTrie a = TrieNode a (Maybe (NodeTrie a)) (Maybe (NodeTrie a))
  deriving (Show, Functor)
pattern Leaf :: a -> NodeTrie a
pattern Leaf a = TrieNode a Nothing Nothing
onLeftTrie :: (Maybe (NodeTrie a) -> NodeTrie a) -> NodeTrie a -> NodeTrie a
onLeftTrie f (TrieNode i l r) = TrieNode i (Just $ f l) r
onRightTrie :: (Maybe (NodeTrie a) -> NodeTrie a) -> NodeTrie a -> NodeTrie a
onRightTrie f (TrieNode i l r) = TrieNode i l (Just $ f r)
onThisTrie :: (Maybe (NodeTrie a) -> NodeTrie a) -> NodeTrie a -> NodeTrie a
onThisTrie f = f . Just

liftFn
  :: Monoid a => (NodeTrie a -> NodeTrie a) -> Maybe (NodeTrie a) -> NodeTrie a
liftFn f = f . fromMaybe (Leaf mempty)
chainOps :: Monoid a => TrieOp a -> TrieOp a -> TrieOp a
chainOps onS onS' f = onS (liftFn $ onS' f)
type TrieOp a = ((Maybe (NodeTrie a) -> NodeTrie a) -> NodeTrie a -> NodeTrie a)
boolToOp :: Bool -> TrieOp a
boolToOp b = if b then onRightTrie else onLeftTrie

pathToOp :: Monoid a => [Bool] -> TrieOp a
pathToOp = foldr (chainOps . boolToOp) onThisTrie

insertToTrie
  :: Node -> NodeTrie [(Int,StringType)] -> NodeTrie [(Int,StringType)]
insertToTrie (Node i path a) =
  pathToOp path $ liftFn $ \(TrieNode as l r) ->
  TrieNode ((i,a) : as) l r
insertToTrie _               = id

fmap2 :: (Functor f, Functor g) => (a -> b) -> f (g a) -> f (g b)
fmap2 = fmap . fmap
{-# INLINE fmap2 #-}

trieToList :: (a -> a -> a) -> NodeTrie a -> [a]
trieToList f = \case
  Leaf a -> [a]
  TrieNode commonPrefix l r -> map (f commonPrefix) $ recur l ++ recur r where
    recur = maybe [] $ trieToList f

nodeId :: Node -> [Bool]
nodeId = \case
  NewPlan _  -> []
  Node _ x _ -> x
nodeString  :: Node -> StringType
nodeString = \case
  NewPlan _  -> undefined
  Node _ _ s -> s
nodeLinum :: Node -> Linum
nodeLinum = \case
  NewPlan _  -> undefined
  Node i _ _ -> i
reprNode :: Node -> String
reprNode n = printf "[%d]'%s'" (nodeLinum n) (C8.unpack $ nodeString n)
toBranches :: [Node] -> [[StringType]]
toBranches =
  fmap2 snd
  . fmap (sortOn fst)
  . trieToList (++)
  . foldl (flip insertToTrie) (Leaf mempty)

uniqueSortedOnKeepLast :: Eq b => (a -> b) -> [a] -> [a]
uniqueSortedOnKeepLast f as = map fst $ foldr go' [] $ zip as $ map f as
  where
    go' a []               = [a]
    go' (y,b) xs@((_,a):_) = if a == b then xs else (y,b) : xs

readFileCached :: FilePath -> StateT [(FilePath, [StringType])] IO [StringType]
readFileCached fn = do
  retM <- gets $ lookup fn
  case retM of
    Nothing -> do
      ret <- lift $ C8.lines <$> B.readFile fn
      modify ((fn,ret) :)
      return ret
    Just ret -> return ret

type Plans = []
type Branches = []
readBranches
  :: FilePath
  -> StateT [(FilePath,[StringType])] IO [(Int,Branches [StringType])]
readBranches = fmap extractBranches . readFileCached where
  extractBranches :: [StringType] -> [(Int, Branches [StringType])]
  extractBranches =
    map (second toBranches)
    -- . uniqueSortedOnKeepLast fst
    . enumeratePlans
    . (catMaybes :: [Maybe Node] -> [Node])
    . zipWith lineToNode [1..]
  enumeratePlans :: [Node] -> [(Int, [Node])]
  enumeratePlans = snd . splitWhen (\case {NewPlan i -> Just i; _ -> Nothing})

readGraphs :: FilePath -> StateT [(FilePath,[StringType])] IO [GraphDescription]
readGraphs = fmap graphDescriptions . readFileCached
readStates :: FilePath -> StateT [(FilePath,[StringType])] IO [StateDescription]
readStates = fmap stateDescriptions . readFileCached
readClusters :: FilePath -> StateT [(FilePath,[StringType])] IO [ClustDescription]
readClusters = fmap clustDescriptions . readFileCached
splitWhen1 :: (a -> Bool) -> [a] -> ([a], [a])
splitWhen1 _ [] = ([],[])
splitWhen1 f (x:xs) | not $ f x = first (x:) $ splitWhen1 f xs
                    | otherwise = ([], x:xs)

type ClustDescription = [StringType]
clustDescriptions :: [StringType] -> [ClustDescription]
clustDescriptions = reverse . (`execState` []) . noClust where
  noClust :: [StringType] -> State [ClustDescription] ()
  noClust []                    = return ()
  noClust ("Begin Clusters":ls) = modify ([]:) >> inClust ls
  noClust (_:ls)                = noClust ls
  inClust []                  = error "End while reading clusters"
  inClust ("End Clusters":ls) = noClust ls
  inClust (l:ls)              = modify (\(x:xs) -> (x ++ [l]):xs) >> inClust ls


type GraphDescription = [StringType]
graphDescriptions :: [StringType] -> [GraphDescription]
graphDescriptions = reverse . (`execState` []) . noGraph where
  noGraph :: [StringType] -> State [GraphDescription] ()
  noGraph [] = return ()
  noGraph (l:ls) = case l of
    "Graph: [" -> modify ([l]:) >> inGraph ls
    _          -> noGraph ls
  inGraph [] = return ()
  inGraph (l:ls) = modify (\(x:xs) -> (x ++ [l]):xs) >> case l of
    "]" -> noGraph ls
    _   -> inGraph ls

type StateDescription = [StringType]
stateDescriptions :: [StringType] -> [StateDescription]
stateDescriptions = reverse . (`execState` []) . noDesc where
  noDesc :: [StringType] -> State [GraphDescription] ()
  noDesc []                          = return ()
  noDesc ("Nodes (size,score): ":ls) = modify ([]:) >> inDesc ls
  noDesc (_:ls)                      = noDesc ls
  inDesc [] = return ()
  inDesc (l:ls) = modify (\(x:xs) -> (x ++ [l]):xs) >> case l of
    "]" -> noDesc ls
    _   -> inDesc ls


-- | Stript the \[[0-9]+\.[0-9]+s\] <line>
stripTime :: StringType -> (Maybe Double,StringType)
stripTime str = maybe (Nothing,str) (first Just) $ do
  rest <- C8.stripPrefix "[" str
  let (intPart,rest) = C8.span isDigit rest
  _rest <- C8.stripPrefix "." rest
  let (decPart,_rest) = C8.span isDigit rest
  _rest <- C8.stripPrefix "s] " rest
  guard $ not $ C8.null intPart || C8.null decPart
  return (read $ C8.unpack $ intPart <> "." <> decPart,rest)

indentLines :: Int -> [StringType] -> (Int, [StringType])
indentLines = go [] where
  go :: [StringType] -> Int -> [StringType] -> (Int, [StringType])
  go _ off [] = (off, [])
  go stack off (l:ls) = if
    | "[Before]" `B.isPrefixOf` l -> recur False (+) $ pushStack . rmBrackets
    | "[After" `B.isPrefixOf` l   -> recur True (-) $ popStack . rmBrackets
    | l == "[NEW STUFF]"          -> (l:) <$> go stack off ls
    | otherwise                   -> recur False const $ const id
    where
      rmBrackets = B.drop 2 . C8.dropWhile (/= ']')
      pushStack = (:)
      popStack p [] = error $ printf "Pop %s from empty stack" $ C8.unpack  p
      popStack p (fr:frs) = if fr == p
        then frs
        else error $ printf "Pop '%s' from stack '%s'" (C8.unpack p) (show $ l:ls)
      recur :: Bool
            -> (Int -> Int -> Int)
            -> (StringType -> [StringType] -> [StringType])
            -> (Int, [StringType])
      recur onCurrent modOff modStack = ((indent <> l):) <$> go stack' off' ls
        where
          indent = C8.replicate (fromIntegral $ if onCurrent then off' else off) ' '
          off' = modOff off 2
          stack' = modStack l stack

enumerate :: [a] -> [(Int, a)]
enumerate = zip [0..]

data BranchResult = BranchFailure | BranchSuccess | BranchResultUnknown
  deriving (Show, Eq)
lastLineToResult :: C8.ByteString -> BranchResult
lastLineToResult x
  | "ERROR" `B.isPrefixOf` x = BranchFailure
  | "BOT" `B.isPrefixOf` x = BranchFailure
  | "Successfully materialized" `B.isPrefixOf` x = BranchSuccess
  | otherwise = BranchResultUnknown

finalNum :: StringType -> Int
finalNum = read' . go where
  go x = if C8.null res then x else go res where
    res = C8.dropWhile (not . isNumber) $ C8.dropWhile isNumber x

safeLast :: a -> [a] -> a
safeLast _ [x]    = x
safeLast e (_:xs) = safeLast e xs
safeLast e []     = e

main :: IO ()
main = (`evalStateT` []) $ do
  -- Brnaches
  dumpFile <- lift $ fromMaybe "/tmp/benchmark.out" . listToMaybe <$> getArgs
  let rootDir = printf "%s.bench_branches" dumpFile
  let branchDirf = printf "%s/branches%03d" rootDir
  lift $ createDirectoryIfMissing True rootDir
  when True $ do
    plans <- readBranches dumpFile
    lift $ putStrLn $ printf "Will write %d plans" (length plans)
    lift $ forM_ (enumerate plans) $ \(i,(nodeSolved,branches)) -> do
      putStrLn
        $ printf
          "\tWriting plan %d (%d branches, node: %d)"
          i
          (length branches)
          nodeSolved
      let branchDir = branchDirf i
      createDirectoryIfMissing True branchDir
      forM_ (enumerate $ zip branches ([] : branches)) $ \(ib,(b,_oldBranch)) -> do
        let fname = printf "%s/branch%04d.txt" branchDir (ib :: Int)
        -- let (old, new) = splitWhen1 (uncurry (/=)) $ zip b $ oldBranch ++ repeat ""
        -- let curatedBranch = fmap fst old ++ ["[NEW STUFF]"] ++ fmap fst new
        let curatedBranch :: [StringType] = b
        let (off,outLines) = indentLines 0 curatedBranch
        let result =
              safeLast BranchResultUnknown $ lastLineToResult <$> curatedBranch
        when (case result of
                BranchSuccess -> True
                _             -> False || ib `mod` 100 == 0)
          $ putStrLn
          $ printf
            "\t\tWrinting branch #%d (%d entries, %s)"
            ib
            (length outLines)
            (show result)
        B.writeFile fname $ B.intercalate "\n" outLines <> "\n"
        -- It doesn't matter of offset is >0.
        when (off < 0) $ fail $ "Bad indent in: " ++ fname
        return $ length outLines
  -- Graphs
  when False $ do
    graphs <- readGraphs dumpFile
    lift $ putStrLn $ printf "Will write %d graphs" (length graphs)
    lift $ forM_ (enumerate graphs) $ \(i,graph) -> do
      putStrLn $ printf "Will write graph #%d" i
      let branchDir = branchDirf i
      createDirectoryIfMissing True branchDir
      B.writeFile (branchDir ++ "/graph.txt") $ B.intercalate "\n" graph
  -- States
  when True $ do
    statess <- readStates dumpFile
    lift $ putStrLn $ printf "Will write %d state sets" (length statess)
    lift $ forM_ (enumerate statess) $ \(i,states) -> do
      putStrLn $ printf "Will write state set #%d" i
      let branchDir = branchDirf i
      createDirectoryIfMissing True branchDir
      B.writeFile (branchDir ++ "/states.txt") $ B.intercalate "\n" states
  -- Clusters
  when False $ do
    clusterss <- readClusters dumpFile
    lift $ putStrLn $ printf "Will write %d cluster sets" (length clusterss)
    lift $ forM_ (enumerate clusterss) $ \(i,clusters) -> do
      putStrLn $ printf "Will write cluster set #%d" i
      let branchDir = branchDirf i
      createDirectoryIfMissing True branchDir
      B.writeFile (branchDir ++ "/clusters.txt") $ B.intercalate "\n" clusters


branches :: IO (Branches [StringType])
branches = fmap (snd . head) $ (`evalStateT` []) $ readBranches "/tmp/benchmark.out"
nodeSolutions :: IO [[Int]]
nodeSolutions = fmap2 (mapMaybe $ intAfter "[Before] setNodeStateSafe <") branches
