{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE LiberalTypeSynonyms        #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE NamedWildCards             #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}
module Data.Query.SQL.Parser (parseSQL) where

import           Control.Applicative
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer
import           Data.Bifunctor
import           Data.Char                  hiding (isSymbol)
import           Data.Either
import           Data.List
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.Query.SQL.Uncorrelate
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Unsafe
import           GHC.Stack
import           Text.Printf


newtype ExceptT err m a = ExceptT {runExceptT :: m (Either err a)}
  deriving Functor
throwError :: Applicative m => err -> ExceptT err m a
throwError = ExceptT . pure . Left
instance Monad m => Applicative (ExceptT err m) where
  pure = return
  (<*>) = ap
instance Monad m => Monad (ExceptT err m) where
  return = ExceptT . return . return
  ExceptT a >>= f = ExceptT $ a >>= runExceptT . either throwError f
instance (Monad m,Alternative m) => Alternative (ExceptT err m) where
  empty = ExceptT empty
  ExceptT a <|> ExceptT b = ExceptT $ a <|> b
instance (Monad m,Alternative m) => MonadPlus (ExceptT err m) where
  mzero = empty
  mplus = (<|>)
instance MonadTrans (ExceptT err) where
  lift = ExceptT . fmap return
instance MonadState s m => MonadState s (ExceptT err m) where
  get = lift get
  put = lift . put
type PError = (String,String)
newtype P a = P (ExceptT PError (StateT String []) a)
  deriving (Functor,Monad,Applicative,MonadPlus,Alternative,MonadSelect)

data Stream e = Stream e (Stream e) deriving Functor
class (Symbolic e,Monad m) => MonadGen e m where pop :: m e
instance (Symbolic e,Monad m) => MonadGen e (StateT (Stream e) m) where
  pop = get >>= \case {Stream a as -> put as >> return a}
instance (Symbolic e',MonadGen e m) => MonadGen e' (ReaderT (e -> e') m) where
  pop = do {f <- ask ; f <$> lift pop}
instance (Monoid w,MonadGen e m) => MonadGen e (WriterT w m) where pop = lift pop

dropPrefix :: String -> String -> Maybe String
dropPrefix s l = if s `isPrefixOf` l then Just $ drop (length s) l else Nothing

class (MonadSelect p,MonadPlus p) => MonadParse p where
  modText :: (String -> String) -> p ()
  default modText
    :: (MonadParse m,MonadTrans t,p ~ t m) => (String -> String) -> p ()
  modText = lift . modText

  getText :: p String
  default getText :: (MonadParse m,MonadTrans t,p ~ t m) => p String
  getText = lift getText

  putText :: String -> p ()
  default putText :: (MonadParse m,MonadTrans t,p ~ t m) => String -> p ()
  putText = lift . putText

  throwText :: HasCallStack => String -> p a
  default throwText
    :: (HasCallStack,MonadParse m,MonadTrans t,p ~ t m) => String -> p a
  throwText = lift . throwText
instance MonadParse m => MonadParse (StateT a m)
instance MonadParse m => MonadParse (ExceptT a m)
instance MonadParse m => MonadParse (ReaderT a m)
instance MonadParse P where
  modText = P . modify
  getText = P get
  putText = P . put
  throwText err = do
    text <- getText
    let stack = intercalate ">"
          $ takeWhile (/= "throwText")
          $ fmap fst
          $ reverse
          $ getCallStack callStack
    P $ throwError (printf "%s: %s" stack err,text)

-- | Like one of the two.
class Monad p => MonadSelect p where
  select0 :: (a -> Bool) -> p a -> p a -> p a
instance MonadSelect p => MonadSelect (StateT s p) where
  select0 isG (StateT f) (StateT g) = StateT $ \s -> select0 (isG . fst) (f s) (g s)
instance MonadSelect p => MonadSelect (ReaderT s p) where
  select0 isG (ReaderT f) (ReaderT g) = ReaderT $ \s -> select0 isG (f s) (g s)
instance (Monoid s,MonadSelect p) => MonadSelect (WriterT s p) where
  select0 isG (WriterT f) (WriterT g) = WriterT $ select0 (isG . fst) f g
instance (MonadSelect p) => MonadSelect (ExceptT s p) where
  select0 isG (ExceptT f) (ExceptT g) =
    ExceptT $ select0 (either (const False) isG) f g
instance MonadSelect [] where
  select0 isG l r = uncurry (++) $ span isG $ l ++ r

select :: MonadSelect p => p a -> p a -> p a
select = select0 $ const True
lit :: (HasCallStack,MonadParse p) => String -> p ()
lit l = do
  s <- getText
  case dropPrefix l s of
    Nothing -> throwText $ printf "Expected '%s'" l
    Just s' -> putText s'
char :: (HasCallStack,MonadParse p) => Char -> p ()
char c = do
  spaces
  getText >>= \case
    [] -> throwText $ printf "Expected '%c' got end of input." c
    c':s -> if c == c'
      then putText s >> spaces
      else throwText $ printf "Expected '%c'" c
eof :: (HasCallStack,MonadParse p) => p ()
eof = getText >>= \case
  [] -> return ()
  _  -> throwText "Expcected end of file"
skipSpaces :: String -> String
skipSpaces = drpSpace where
  drpSpace :: String -> String
  drpSpace = drpComment . dropWhile isSpace
  drpComment :: String -> String
  drpComment = \case
    '-':'-':xs -> drpSpace $ dropWhile (/= '\n') xs
    xs         -> xs
spaces :: MonadParse p => p ()
spaces = modText skipSpaces

parens :: (HasCallStack,MonadParse p) => p a -> p a
parens m = do {char '('; r <- m; char ')'; return r}
sep1 :: (HasCallStack,MonadParse p) => p x -> p a -> p [a]
sep1 s m = do {r <- m; (s >> fmap (r:) (sep1 s m)) <|> return [r]}
word :: (HasCallStack,MonadParse p) => String -> p ()
word w = do {spaces ; r <- lit w ; spaces ; return r}
readWhile :: (HasCallStack,MonadParse p) => (Char -> Bool) -> p String
readWhile f = getText >>= (\case
  ([],_)  -> throwText "Failed readWhile."
  (ret,p) -> putText p >> return ret) . span f

parseSym :: (HasCallStack,MonadParse p) => p String
parseSym = getText >>= (\case
  ([],xs)  -> throwText $ printf "Expected symbol at '%s'" xs
  (s,rest) -> putText rest >> return s) . span (\c -> isAlphaNum c || c == '_')

parse :: P a -> String -> Either [PError] a
parse (P (ExceptT (StateT f))) s = case partitionEithers $ fst <$> f s of
  (errs,[]) -> Left errs
  (_,a:_)   -> Right a
parseMaybe :: MonadSelect p => p a -> p (Maybe a)
parseMaybe m = fmap Just m `select` return Nothing

parseFrom :: (HasCallStack,MonadParse p,Eq e) =>
            (String -> e -> Maybe e) -> p (Query e s) -> p ([e] -> Query e s)
parseFrom rmPref s = do
  word "from"
  tbls <- sep1 (word ",") $ parseTableAsClause rmPref s
  return $ \es -> foldl1 (Q2 QProd) (($ es) <$> tbls)
parseTableAsClause
  :: (HasCallStack,MonadParse p,Eq e)
  => (String -> e -> Maybe e)
  -> p (Query e s)
  -> p ([e] -> Query e s)
parseTableAsClause rmPref sM = do
  r <- sM
  parseMaybe (word "as" >> parseSym) >>= \case
    Nothing -> return $ const r
    Just pref -> return
      $ \es -> Q1 (QProj $ nub $ mapMaybe (\e -> (e,) . E0 <$> rmPref pref e) es) r
parseWhere :: (HasCallStack,MonadParse p) => p e -> p (Query e s -> Query e s)
parseWhere eM = do
  word "where"
  p <- parsePropRelExpr eM
  return $ S p
parseLimit :: (HasCallStack,MonadParse p) => p (Query e s -> Query e s)
parseLimit = do
  i <- word "limit" >> fromIntegral <$> parseInt
  return $ if i < 0 then id else Q1 (QLimit i)
parseNat :: (HasCallStack,MonadParse p) => p Int
parseNat = read <$> readWhile isDigit
parseSelectProj
  :: (HasCallStack,MonadGen e p,MonadParse p)
  => p e
  -> p (Query e s -> Query e s)
parseSelectProj eM = do
  word "select"
  fmap (Q1 . QProj) $ sep1 (word ",") $ do
    ex <- parseExpr (E0 <$> eM)
    parseMaybe (word "as" >> eM) >>= \case
      Just e -> return (e,ex)
      Nothing -> do
        e <- maybe pop (return . mkSymbol) $ asSymbol ex
        return (e,ex)

parseFromLeftOuterJoin :: (HasCallStack,MonadParse p) =>
                         p e -> p (Query e s) -> p (Query e s)
parseFromLeftOuterJoin eM sM = do
  l <- word "from" >> sM
  r <- word "left" >> word "outer" >> word "join" >> sM
  p <- word "on" >> parsePropRelExpr eM
  return $ J p l r

-- | `parseSelectGroup` `parseGroupBy` and `pareseHaving`
parseSelectGroup :: (HasCallStack,MonadGen e p,MonadParse p) =>
                   p e -> p [(e,Expr (Aggr (Expr e)))]
parseSelectGroup eM = do
  word "select"
  sep1 (word ",") $ do
    ex <- parseExpr $ fmap E0 $ parseAggr $ parseExpr $ E0 <$> eM
    parseMaybe (word "as" >> eM) >>= \case
      Just e -> return (e,ex)
      Nothing -> do
        e <- maybe pop (return . mkSymbol) $ asSymbol ex
        return (e,ex)

parseGroupBy :: (HasCallStack,MonadParse p) => p e -> p [Expr e]
parseGroupBy eM = do
  word "group" >> word "by"
  sep1 (spaces >> char ',' >> spaces) $ parseExpr $ E0 <$> eM

-- | Populate the group with extra symbols.
parseHaving :: (HasCallStack,MonadParse p,MonadGen e p) =>
              p e
            -> p (Query e s -> Query e s,[(e,Aggr (Expr e))])
parseHaving eM = do
  word "having"
  p0 <- parsePropRelExpr $ parseAggr $ parseExpr $ E0 <$> eM
  (p,es) <- runWriterT $ fmap3 join $ traverse3 aggrSymM p0
  return (Q1 $ QSel p,es)
  where
    aggrSymM = \case
      NAggr AggrFirst e -> return e
      ag                -> do {e <- pop;tell [(e,ag)];return $ E0 e}

parseOrderBy :: (HasCallStack,MonadParse p) => p e -> p (Query e s -> Query e s)
parseOrderBy eM = do
  word "order" >> word "by"
  es <- sep1 (char ',') $ do
    r <- parseExpr $ E0 <$> eM
    dir <- fmap (maybe id $ const $ E1 ENeg) $ parseMaybe $ word "desc"
    return $ dir r
  return $ Q1 $ QSort es

parseSelectStar :: MonadParse p => p (Query e s -> Query e s)
parseSelectStar = word "select" >> word "*" >> return id

parseNestedQueryE
  :: (HasCallStack,MonadParse p)
  => p e
  -> p (Query (NestedQueryE s e) s)
  -> p (NestedQueryE s e)
parseNestedQueryE eM qM =
  fmap NestedQueryE $ Right <$> eM <|> Left <$> parens qM <|> prsIn
  where
    -- x in (select a from <query>) => exists (select * from (<query>) where x = a)
    prsIn = do
      e <- eM
      word "in"
      q <- parens qM
      sym <- exposedSymbol q
      return
        $ Left $ S (P0 (R2 REq (R0 (E0 $ NestedQueryE $ Right e)) (R0 (E0 sym)))) q
    -- assume the exposed symbol is a) unique b) there is exactly one
    -- exposed (the rest are helpers exposed by us)
    exposedSymbol = \case
        Q1 (QGroup ((l,_):_) _) _ -> return l
        Q1 (QProj ((l,_):_)) _ -> return l
        Q1 _ q -> exposedSymbol q
        q -> throwText $ "Expected a query with unique symbols, not "
            ++ show (bimap (const ()) (const ()) q)
parseQuery
  :: (HasCallStack,MonadGen e p,MonadParse p,Eq e)
  => (String -> e -> Maybe e)
  -> p e
  -> p (Query e s)
  -> p (Query e s)
parseQuery rmPref eM sM = do
  putProjE <- Left <$> (select parseSelectStar $ parseSelectProj eM)
             <|> Right <$> parseSelectGroup eM
  prod <- parseFrom rmPref (select sM $ parens $ parseQuery rmPref eM sM)
    `select` fmap const (parseFromLeftOuterJoin eM sM)
  putSel <- fmap (fromMaybe id) $ parseMaybe $ parseWhere eM
  putProj <- case putProjE of
    Left x -> return x
    Right grpProj -> do
      exps <- fmap (fromMaybe []) $ parseMaybe $ parseGroupBy eM -- we NEED this
      (havingSel,extraProj) <-
        fmap (fromMaybe (id,[])) $ parseMaybe $ parseHaving eM
      return $ havingSel . Q1 (QGroup (grpProj ++ fmap2 E0 extraProj) exps)
  putSort <- fmap (fromMaybe id) $ parseMaybe $ parseOrderBy eM
  putLimit <- (fromMaybe id) <$> parseMaybe parseLimit
  spaces
  return $ join $ flattenQ $ putLimit $ putSort $ putProj $ putSel $ Q0 $ prod

flattenQ :: Query e ([e] -> s) -> Query e s
flattenQ = go [] where
  go es = \case
    Q0 s     -> Q0 $ s es
    Q1 o l   -> Q1 o $ go (toList o ++ es) l
    Q2 o l r -> Q2 o (go (toList o ++ es) l) (go (toList o ++ es) r)

parseNestedQuery
  :: forall p e s .
  (HasCallStack,Symbolic e,Eq e,Eq s,MonadParse p)
  => (String -> e -> Maybe e)
  -> p e
  -> p s
  -> StateT (Stream e) p (Query (NestedQueryE s e) s)
parseNestedQuery f eM sM =
  runReaderT (runRK qM) $ NestedQueryE . Right
  where
    qM :: RK
         (ReaderT (e -> NestedQueryE s e) (StateT (Stream e) p))
         (Query (NestedQueryE s e) s)
    qM = parseQuery f' (parseNestedQueryE (lift3 eM) qM) $ lift3 $ Q0 <$> sM
    f' pref (NestedQueryE (Right e)) = NestedQueryE . Right <$> f pref e
    f' _ _                           = Nothing

-- | The following should be greedy: parseProp (parseProp ...) should
-- yield P0 for the inner prop ONLY. Don't backtrack.
parsePropRelExpr
  :: forall p e . (HasCallStack,MonadParse p) => p e -> p (Prop (Rel (Expr e)))
parsePropRelExpr eM =
  parseProp $ select (parseExists eM) $ parseRel $ parseExpr $ E0 <$> eM
parseExists :: (MonadParse p,HasCallStack) => p e -> p (Prop (Rel (Expr e)))
parseExists eM = do
  putNot <- maybe (P1 PNot) (const id) <$> parseMaybe (word "not")
  word "exists"
  putNot . return3 <$> eM
parseProp :: (HasCallStack,MonadParse p) => p (Prop a) -> p (Prop a)
parseProp pM = tokTree (select pM $ parens $ parseProp pM)
  [word "and" >> return And,word "or" >> return Or]
-- | This can do `in`, `not in`, `not like` `<>`
parseRel :: (HasCallStack,MonadParse p) => p a -> p (Prop (Rel a))
parseRel rM =  do
  l <- R0 <$> rM
  select (parseBetween l rM) $ select (parsePropIn l rM) $ flip select (return $ P0 l) $ do
    op <- foldr1Unsafe select [
      word "<=" >> return (P0 ... R2 RLe),
      word ">=" >> return (P0 ... R2 RGe),
      word "<>" >> return ((P1 PNot . P0) ... R2 REq),
      word "="  >> return (P0 ... R2 REq),
      word ">"  >> return (P0 ... R2 RGt),
      word "<" >> return (P0 ... R2 RLt),
      word "like" >> return (P0 ... R2 RLike),
      word "not" >> word "like" >> return ((P1 PNot . P0) ... R2 RLike)]
    r <- R0 <$> rM
    return $ op l r
parsePropIn :: (HasCallStack,MonadParse p) => Rel a -> p a -> p (Prop (Rel a))
parsePropIn ex eM = do
  p <- fmap (maybe id (const $ P1 PNot)) $ parseMaybe $ word "not"
  xs <- word "in" >> parens (sep1 (word ",") $ R0 <$> eM)
  return $ p $ foldr1Unsafe Or (P0 . R2 REq ex <$> xs)
parseBetween :: (HasCallStack,MonadParse p) => Rel a -> p a -> p (Prop (Rel a))
parseBetween ex rM = do
  word "between"
  fr <- R0 <$> rM
  word "and"
  to <- R0 <$> rM
  return $ And (P0 (R2 RLe fr ex)) (P0 (R2 RLe ex to))

propToExpr :: Prop e -> Expr e
propToExpr = \case
  P2 POr l r  -> E2 EOr (propToExpr l) (propToExpr r)
  P2 PAnd l r -> E2 EAnd (propToExpr l) (propToExpr r)
  P1 PNot e   -> E1 ENot $ propToExpr e
  P0 e        -> E0 e
relToExpr :: Rel e -> Maybe (Expr e)
relToExpr = \case
  R2 o l r -> do
    o' <- case o of
      REq   -> Just EEq
      RGt   -> Just ELike
      RLike -> Just ELike
      _     -> Nothing
    E2 o' <$> relToExpr l <*> relToExpr r
  R0 e -> return $ E0 e

-- | Pass mzero as `prsRel` to avoid looking for equalities. Note that
-- for recursion internally we use normal `parseExpr` as between
-- parens etc we are in a safe zion
parseExpr :: forall p a . (HasCallStack,MonadParse p) => p (Expr a) -> p (Expr a)
parseExpr prsSym = tokTree prsTerm prsBOps where
  prsTerm :: HasCallStack => p (Expr a)
  prsTerm = foldr1Unsafe (<|>)
    [parens $ parseExpr prsSym,prsEFun,prsCond,prsNeg,prsSym]
    where
      -- case when p then x else y
      prsCond = do
        word "case" >> word "when"
        p <- fmap2 join <$>parsePropRelExpr prsSym
        pe <- case join . propToExpr <$> traverse (fmap join . relToExpr) p of
          Nothing  -> throwText "Can't convert prop to expr"
          Just pe0 -> return pe0
        t <- word "then" >> parseExpr prsSym
        e <- word "else" >> parseExpr prsSym
        word "end"
        return (E2 EOr (E2 EAnd pe t) e)
      prsNeg = word "-"
        >> E1 ENeg <$> select (parens (parseExpr prsSym)) prsSym
  prsBOps :: HasCallStack => [p (Expr a -> Expr a -> Expr a)]
  prsBOps = [
    E2 <$> select (char '*' >> return EMul) (char '/' >> return EDiv),
    E2 <$> select (char '+' >> return EAdd) (char '-' >> return ESub)]
  prsEFun = foldr1Unsafe select [prsSubStr,prsDateExtr] where
    prsDateExtr = (word "extract" >>) $ parens $ do
      fn <- foldr1Unsafe select [
        word "year" >> return ExtractYear,
        word "month" >> return ExtractMonth,
        word "month" >> return ExtractMonth]
      ex <- word "from" >> parseExpr prsSym
      return $ E1 (EFun fn) ex
    prsSubStr = do
      word "substring"
      parens $ do
        str <- parseExpr prsSym
        fr <- word "from" >> parseNat
        len <- word "for" >> parseNat
        return $ E1 (EFun $ SubSeq fr $ fr + len) str

-- | Respect the prescedence. [*,/,+,-]
-- parse (tokTree (char 'a' >> return [()]) [char '+' >> return (++)]) "a"
--
-- This first parses each token and operand "a + 2 * 3 + 7 / 4"
-- interleaving them and then does a sort of "quicksort", at each step
-- splitting the prescedence: ev headExpr t:tail -> split tail in a
-- prefix that has higher prescednence than t and a suffix.  Recurse
-- in the prefix and combin with headExpr and recur that with the
-- suffix.
tokTree
  :: forall p a .
  (HasCallStack,MonadParse p)
  => p a
  -> [p (a -> a -> a)]
  -> p a
tokTree p1 pops = do
  a <- p1
  ev a <$> prsChain
  where
    ev a [] = a
    ev a0 [((_i,f),a1)] = f a0 a1
    ev a0 (((i,f),a1):rest) = ev (f a0 (ev a1 lRest)) gRest where
      (lRest,gRest) = span ((< i) . fst . fst) rest
    prsChain :: p [((Int,a -> a -> a),a)]
    prsChain = parseMaybe ((,) <$> anyOp <*> p1)
      >>= maybe (return []) (\x -> (x:) <$> prsChain)
    anyOp :: p (Int,a -> a -> a)
    anyOp = foldl' select empty $ zipWith (\i -> fmap (i,)) [0..] pops

-- | Replace "count(*)" with "sum(1)" on the fly
parseAggr :: (HasCallStack,MonadParse p) => p a -> p (Aggr a)
parseAggr aM = foldr1Unsafe select [
  countDistinct,
  countStar,
  prsF "sum" AggrSum,
  prsF "avg" AggrAvg,
  prsF "min" AggrMin,
  prsF "max" AggrMax,
  NAggr AggrFirst <$> aM]
  where
    prsF fnName fnOp = word fnName >> NAggr fnOp <$> parens aM
    countDistinct = word "count"
      >> parens (word "distinct" >> NAggr AggrCount <$> aM)
    countStar = do
      word "count" >> parens (word "*")
      tmp <- getText
      putText $ "sum(1)" ++ tmp
      parseAggr aM

-- | Sort of a right kan. RK *MUST* be the outermost monad otherwise
-- select will swallow all previously thrown errors.
newtype RK m a = RK { unRK :: forall b . (a -> m b) -> m b }
  deriving Functor
instance Applicative (RK m) where
  (<*>) = ap
  pure = return
instance Monad (RK m) where
  return x = RK $ \k -> k x
  m >>= k = RK $ \c -> unRK m $ \a -> unRK (k a) c
instance MonadTrans RK where lift m = RK (m >>=)
instance (MonadPlus p,MonadSelect p) => MonadSelect (RK p) where
  select0 isG (RK l) (RK r) = RK $ \f ->
    select0 (const True) (l $ \a -> guard (isG a) >> f a) (r f)

instance MonadGen e m => MonadGen e (RK m) where pop = lift pop
instance MonadPlus p => MonadPlus (RK p) where
  mzero = lift mzero
  mplus (RK l) (RK r) = RK $ \f -> mplus (l f) (r f)
instance MonadParse p => MonadParse (RK p)
instance Alternative p => Alternative (RK p) where
  empty = RK $ const empty
  RK l <|> RK r = RK $ \f -> l f <|> r f
runRK :: Applicative m => RK m a -> m a
runRK (RK f) = f pure

parseExpTypeSym :: (HasCallStack,MonadParse p) => p ExpTypeSym
parseExpTypeSym =
  foldr1Unsafe
    select
    [EDate <$> parseDate
    ,EInterval <$> parseInterval
    ,EFloat <$> parseFloat
    ,EInt <$> parseInt
    ,EString <$> parseString
    ,ESym <$> parseDotSym
    ,EBool <$> parseBool]

parseDotSym :: (HasCallStack,MonadParse p) => p String
parseDotSym = do
  s <- parseSym
  fmap (maybe s (printf "%s.%s" s)) $ parseMaybe $ char '.' >> parseSym
parseBool :: (HasCallStack,MonadParse p) => p Bool
parseBool = (word "True" >> return True) <|> (word "False" >> return False)

-- XXX: expand
parseString :: (HasCallStack,MonadParse p) => p String
parseString = do
  char '\''
  str <- readWhile (/= '\'')
  char '\''
  return str
parseDate :: (HasCallStack,MonadParse p) => p Date
parseDate = word "date" >> parseDate' zeroDay
parseInterval :: (HasCallStack,MonadParse p) => p Date
parseInterval = word "interval" >> parseDate' zeroDay

parseInt :: (HasCallStack,MonadParse p) => p Integer
parseInt = do
  sign <- fmap (maybe 1 (const $ negate 1)) $ parseMaybe $ char '-'
  void $ parseMaybe $ char '-'
  (* sign) . read <$> readWhile isDigit

-- XXX: proper date offset. With carry and everything: to int and back
parseDate' :: (HasCallStack,MonadParse p) => Date -> p Date
parseDate' off@Date{..} = date' <|> interval where
  date' = do {char '\'';d <- dashDate;char '\'';return d}
  interval = do
    n <- do {char '\'';r <- parseInt;char '\'';return r}
    (word "year" >> return off{year=year + n})
      -- 0 indexed
      <|> (word "day" >> return off{day=day + n})
      <|> (word "month" >> return off{month=month + n})
      <|> (word "hour" >> return off{hour=hour + n})
      <|> (word "minute" >> return off{minute=minute + n})
      <|> (word "second" >> return off{seconds=seconds + n})
  dashDate = do
    y <- parseInt
    _ <- char '-'
    m <- parseInt
    _ <- char '-'
    d <- parseInt
    return $ off{day=day+d,month=month+m,year=year+y}

parseFloat :: (HasCallStack,MonadParse p) => p Double
parseFloat = do
  sign <- fmap (maybe 1 (const $ negate 1)) $ parseMaybe $ char '-'
  void $ parseMaybe $ char '-'
  nat :: Int <- read <$> readWhile isDigit
  _ <- char '.'
  fl <- foldr (\d x -> x / 10.0 + fromIntegral (ord d - ord '0') / 10.0) 0.0
    <$> readWhile isDigit
  return $ sign * (fromIntegral nat + fl)

intStream :: Int -> Stream Int
intStream i = Stream i $ intStream $ i+1

parseTableSym :: HasCallStack => MonadParse p => p Table
parseTableSym = TSymbol <$> parseSym

-- aprint $ parseSQL (\_ _ -> Nothing) "select * from A where exists (select *  from B)"
-- aprint $ parseSQL (\_ _ -> Nothing) "select * from A where a=(select min(b) from B)"
-- aprint $ parseSQL (\(ESym x) q -> Just $ x `elem` map unTable (toList q)) "select * from a where not exists (select * from b where a = b)"
-- aprint $ parseSQL (\_ _ -> Nothing) "select a as A from A where a between 1 and 2"
-- aprint $ parseSQL (\_ _ -> Nothing) "select * from (select a from A)"
-- aprint $ parseSQL (\_ _ -> Nothing) "select case, when a = b and b <> c then 1 else 2 end from A"
-- aprint $ parseSQL (\(ESym x) q -> Just $ x `elem` map unTable (toList q)) "select * from a where not exists (select * from b where a = b)"
parseSQL :: (ExpTypeSym -> Query ExpTypeSym Table -> Maybe Bool)
         -> String
         -> Either String (Query ExpTypeSym Table)
parseSQL isInS str =
  first mkParseError
  $ (>>= first (return . (,"") . ashow) . unnestQuery isInS)
  $ (`parse` str)
  $ (`evalStateT` fmap (ESym . printf "tmpSym%d") (intStream 0))
  $ do
    r <- parseNestedQuery rmPref parseExpTypeSym parseTableSym
    eof
    return r
  where
    rndr [] = undefined
    rndr errs@((_,txt):_) = printf "%s\nText: '%s'" body txt
      where
        body =
          unlines $ fmap fst $ takeWhile ((== length txt) . length . snd) errs
    mkParseError = fromString . rndr . reverse . sortOn (negate . length . snd)
    rmPref pref (ESym esym) = ESym <$> dropPrefix (pref ++ ".") esym
    rmPref _ _              = Nothing
