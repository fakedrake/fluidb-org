{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE EmptyCase             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
module Data.Utils.AShow.ARead
  ( aread
  , garead'
  , ARead(..)
  , AReadV
  , ARead2
  , genericARead'
  , areadCase'
  ) where

import           Control.Applicative     hiding (Const)
import           Control.Monad
import           Data.Bifunctor
import           Data.Char
import           Data.Functor.Identity
import qualified Data.HashMap.Lazy       as HM
import qualified Data.HashSet            as HS
import qualified Data.IntMap             as IM
import qualified Data.IntSet             as IS
import           Data.List
import qualified Data.List.NonEmpty      as NEL
import           Data.Monoid
import           Data.Proxy
import qualified Data.Set                as DS
import           Data.Utils.AShow.Common
import           Data.Utils.Compose
import           Data.Utils.Const
import           Data.Utils.Default
import           Data.Utils.Hashable
import           Data.Utils.Tup
import           GHC.Generics
import           Text.Read

class GARead f where
  garead' :: SExp -> Either String (f a)

class GSelListRead f where
  gSelListRead :: [(Maybe String, SExp)] -> Either String (f a)
instance GSelListRead U1 where
  gSelListRead [] = Right U1
  gSelListRead _  = Left "Failed: gSelListRead => U1"
instance (GARead a, Selector c) => GSelListRead (M1 S c a) where
  gSelListRead ((nameM, expr):rest) = case M1 <$> garead' expr of
    Right ret -> if maybe True (== selName ret) nameM
                then Right ret
                else gSelListRead rest
    Left _ -> gSelListRead rest
  gSelListRead []                   = Left "failed sel read"
instance (GSelListRead a, GSelListRead b) => GSelListRead (a :*: b) where
  gSelListRead [] = Left "Failed: gSelListRead => a :*: b"
  gSelListRead as = (:*:) <$> l <*> r where
    l :: Either String (a x)
    l = gSelListRead as
    r :: Either String (b x)
    r = gSelListRead as

instance (GARead a, Datatype c) => GARead (M1 D c a) where
  garead' expr = M1 <$> garead' expr

instance (GSelListRead a, Constructor c) => GARead (M1 C c a) where
  -- garead' c@(M1 x) =
  --   if conIsRecord c
  --   then recSexp (conName c) $ gSelList x
  --   else case gSelList x of
  --     []   -> Sym $ conName c
      -- args -> sexp (conName c) $ snd <$> args
  garead' = \case
    Rec name selList -> do
      ret :: M1 C c a x <- fmap M1 $ gSelListRead $ first Just <$> selList
      if conIsRecord ret && conName ret == name
        then return ret
        else Left "Failed: read record"
    Sym name -> do
      ret :: M1 C c a x <- M1 <$> gSelListRead []
      if conName ret == name
        then return ret
        else Left "Failed: read sym"
    Sub (Sym name:args) -> do
      ret :: M1 C c a x <- case args of
        [] -> garead' $ Sym name
        xs -> M1 <$> gSelListRead ((Nothing,) <$> xs)
      if conName ret == name
        then return ret
        else Left "Failed: read sym"
    _ -> Left "Not a constructor"

instance GARead U1 where
  garead' = \case
    Sub [] -> Right U1
    _      -> error "Not a U1"

-- | Sums: encode choice between constructors
instance (GARead a, GARead b) => GARead (a :+: b) where
  garead' x = case garead' x :: Either String (a x) of
    Right x' -> Right $ L1 x'
    Left e1 -> case garead' x :: Either String (b x) of
      Right x' -> Right $ R1 x'
      Left e2  -> Left $ e1 ++ e2
instance ARead a => GARead (Rec0 a) where
  garead' r = K1 <$> maybe (Left "Failed Rec0") Right (aread' r)

class ARead a where
  aread' :: SExp -> Maybe a
  default aread' :: (Generic a, GARead (Rep a)) => SExp -> Maybe a
  aread' = genericARead'

-- | We want to be able to override the generic behaviour with our own.
genericARead' :: (GARead (Rep a), Generic a) => SExp -> Maybe a
genericARead' = fmap to . either (const Nothing) Just . garead'

-- | Given a string find the possible matches
newtype Parser a = Parser {runParser' :: String -> [(a,String)]}
  deriving Functor

replaceSpace :: String -> String
replaceSpace = reverse . go where
  go :: String -> String
  go = (`foldl'` []) $ curry $ \case
    (xs@(' ':_),c) -> if isSpace c then xs else c:xs
    (xs,c)         -> if isSpace c then ' ':xs else c:xs
runParser :: Parser a -> String -> Maybe (a,String)
runParser p s = case runParser' p $ replaceSpace s of
  []  -> Nothing
  x:_ -> Just x
aread :: ARead a => String -> Maybe a
aread = aread' . fst <=< runParser (do {r <- readSExp;readEOF;return r})
instance Monad Parser where
  return x = Parser $ return . (x,)
  p >>= f = Parser $ \s -> do
    (x,rest) <- runParser' p s
    runParser' (f x) rest
instance Applicative Parser where
  pure = return
  (<*>) = ap
instance Alternative Parser where
  Parser l <|> Parser r = Parser $ \s -> l s <|> r s
  empty = Parser $ const empty
readCharsGreedy :: (Char -> Bool) -> Parser String
readCharsGreedy f = Parser $ \case
  [] -> empty
  xs -> case span f xs of
    ([],_) -> empty
    xs'    -> return xs'
readChar :: (Char -> Bool) -> Parser Char
readChar f = Parser $ \case
  []   -> empty
  x:xs -> if f x then return (x,xs) else empty
readString :: String -> Parser String
readString str = sequenceA $ readChar . (==) <$> str

readEOF :: Parser ()
readEOF = Parser $ \case {[] -> return ((),[]); _ -> empty}
-- | Read a number of characters that would satisfy.
readRepeat :: Parser a -> Parser [a]
readRepeat p = go where
  go = do
    rM <- (Just <$> p) <|> return Nothing
    case rM of
      Just r  -> (r:) <$> go
      Nothing -> return []
readIntersp1 :: Parser () -> Parser a -> Parser [a]
readIntersp1 sep p = do
  h <- p
  t <- readRepeat (sep >> p)
  return $ h:t

readSpaces :: Parser ()
readSpaces = void (readCharsGreedy isSpace) <|> return ()
readBetween :: (Char,Char) -> Parser a -> Parser a
readBetween (s,e) p = do
  void $ readChar (== s)
  r <- p
  void $ readChar (== e)
  return r
stripSpaces :: Parser a -> Parser a
stripSpaces p = do {readSpaces; r <- p; readSpaces; return r}

readSymSexp :: Parser String
readSymSexp =
  readCharsGreedy $ \c -> isAlphaNum c || c == '\'' || c == '.' || c == '_'
readStringLit :: Parser String
readStringLit = fmap join
  $ readBetween ('"','"')
  $ readRepeat
  $ readString "\\\"" <|> fmap return (readChar (/= '"'))
readSymPair :: Parser a -> String -> Parser (String,a)
readSymPair m sep = do
  s <- readSymSexp
  readSpaces
  void $ readString sep
  readSpaces
  val <- m
  return (s,val)

readListOf :: (Char,Char) -> String -> Parser a -> Parser [a]
readListOf rang sep p = emptyP <|> nonEmptyP
  where
    emptyP = readBetween rang readSpaces >> return []
    nonEmptyP =
      readBetween rang (readIntersp1 (void $ readString sep) (stripSpaces p))
readCaseSexpP :: Parser a -> Parser [(String,a)]
readCaseSexpP p = do
  _ <- readString "\\case"
  readSpaces
  readListOf ('{','}') ";" $ readSymPair p "->"

readRecSexpP :: Parser a -> Parser (String, [(String,a)])
readRecSexpP p = do
  rsym <- readSymSexp
  readSpaces
  body <- readListOf ('{','}') "," $ readSymPair p "="
  return (rsym,body)

readVecSexpP :: Parser a -> Parser [a]
readVecSexpP = readListOf ('[',']') ","
readTupSexpP :: Parser a -> Parser [a]
readTupSexpP p = do
  void $ readChar (== '(')
  h <- stripSpaces p
  h1 <- readChar (== ',') >> stripSpaces p
  t <- readRepeat $ readChar (== ',') >> stripSpaces p
  void $ readChar (== ')')
  return $ h:h1:t

readSExpNoParen :: Parser SExp
readSExpNoParen =
  uncurry Rec <$> readRecSexpP readSExp
  <|> (Str <$> readStringLit)
  <|> (Case <$> readCaseSexpP readSExp)
  <|> (Vec <$> readVecSexpP readSExp)
  <|> (Tup <$> readTupSexpP readSExp)
  <|> (Sym <$> readSymSexp)
readSExpParen :: Parser SExp
readSExpParen = readBetween ('(',')') $ stripSpaces readSExp

readSExpInner :: Parser SExp
readSExpInner = readSExpNoParen <|> readSExpParen

readSExp :: Parser SExp
readSExp = do
  exps <- readIntersp1 (readChar (== ' ') >> readSpaces) readSExpInner
  case exps of
    []  -> error "unreachable"
    [x] -> return x
    xs  -> return $ Sub xs

symRead :: Read a => SExp -> Maybe a
symRead = \case
  Sym s -> readMaybe s
  _     -> Nothing
instance ARead SExp where aread' = Just
instance ARead Bool where aread' = symRead
instance ARead Int where aread' = symRead
instance ARead Integer where aread' = symRead
instance ARead Char where aread' = symRead
instance ARead Double where aread' = symRead
instance ARead Float where aread' = symRead
instance ARead () where
  aread' = \case {Tup [] -> Just (); Sub [] -> Just (); _ -> Nothing}
instance (ARead a, ARead b) => ARead (a,b) where
  aread' = \case
    Tup [a,b] -> (,) <$> aread' a <*> aread' b
    _         -> Nothing
instance (ARead a, ARead b, ARead c) => ARead (a,b,c) where
  aread' = \case
    Tup [a,b,c] -> (,,) <$> aread' a <*> aread' b <*> aread' c
    _           -> Nothing
instance ARead a => ARead (Maybe a)
instance (ARead a, ARead b) => ARead (Either a b)
instance ARead (f (g a)) => ARead (Compose f g a)
instance ARead a => ARead (Identity a)
instance ARead a => ARead (Const a b)
instance ARead a => ARead (Sum a)
instance ARead b => ARead (CoConst a b)
instance ARead a => ARead (Tup2 a)

class AReadList vt a where
  areadList' :: Proxy vt -> SExp -> Maybe [a]
instance ARead a => AReadList NormalVec a where
  areadList' _ = \case
    Vec as -> aread' `traverse` as
    _      -> Nothing
instance AReadList StringVec Char where
  areadList' _ = \case
    Str str -> Just str
    _       -> Nothing
instance AReadList (VecType a) a => ARead [a] where
  aread' = areadList' (Proxy :: Proxy (VecType a))
instance (ARead a,ARead b,Hashable a,Eq a) => ARead (HM.HashMap a b) where
  aread' = \case
    Sub [Sym "HM.fromList",p] -> HM.fromList <$> aread' p
    _                         -> Nothing
instance (ARead a) => ARead (IM.IntMap a) where
  aread' = \case
    Sub [Sym "IM.fromList", p] -> IM.fromList <$> aread' p
    _                          -> Nothing
instance (Hashable a, Eq a, AReadList (VecType a) a) => ARead (HS.HashSet a) where
  aread' = \case
    Sub [Sym "HS.fromList", p] -> HS.fromList <$> aread' p
    _                          -> Nothing
instance ARead IS.IntSet where
  aread' = \case
    Sub [Sym "IS.fromList", p] -> IS.fromList <$> aread' p
    _                          -> Nothing
instance (AReadList (VecType a) a, ARead a, Ord a) => ARead (DS.Set a) where
  aread' = \case
    Sub [Sym "DS.fromList", p] -> DS.fromList <$> aread' p
    _                          -> Nothing

type AReadV a = (ARead a, AReadList (VecType a) a)

data Re = Re { ra :: Int
             , rd :: ()
             } deriving (Generic,Show)
instance Default Re
instance AShow Re
instance ARead Re
instance AReadV a => ARead (NEL.NonEmpty a)
areadCase' :: forall a b . (Eq a, Read a,ARead b) => SExp -> Maybe (a -> b)
areadCase' = \case
  Case xs -> do
    lut <- go xs
    return $ fromJustErr . (`lookup` lut)
  _       -> Nothing
  where
    fromJustErr (Just x) = x
    fromJustErr Nothing  = error "aread: \\case read failed."
    go :: [(String,SExp)] -> Maybe [(a,b)]
    go = \case
      [] -> Just []
      (t,vM):xs -> do
        v <- aread' vM
        ((read t,v) :) <$> go xs

type ARead2 a b = (AReadV a,AReadV b)
