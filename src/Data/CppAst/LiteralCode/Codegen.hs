{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.LiteralCode.Codegen
  ( CodegenIndent(..)
  , Codegen(..)
  , IsCode
  , Record
  , SymbolCode
  , SoftFilePath
  , Offset
  , SymbolId
  , toCodeIndent
  , hardenLiteralCode
  , putLine
  , withOffset
  ) where

import           Control.Monad.State
import           Control.Monad.Writer
import           Data.Char
import           Data.CppAst.LiteralCode.LiteralCode
import           Data.CppAst.LiteralCode.SoftUNameCode
import           Data.String
-- import           Data.Query.Algebra

type Offset c = StateT Int (Writer c)
type IsCode a = (IsString a, Monoid a)
class IsCode c => CodegenIndent a c where
  toCodeIndent' :: a -> Offset c ()

putLine :: IsCode c => c -> Offset c ()
putLine c = do
  off <- get
  tell $ mconcat $ replicate off " "
  tell c
  tell "\n"

withOffset :: Monoid c => Int -> Offset c a -> Offset c a
withOffset off st = do
  modify (+ off)
  ret <- st
  modify $ \x -> x - off
  return ret

hardenLiteralCode :: Codegen a LiteralCode => SoftUNameCode' a -> LiteralCode
hardenLiteralCode = snd . commitUNames' Nothing . fmap toCode
toCodeIndent :: CodegenIndent a c => a -> c
toCodeIndent = execWriter . (`evalStateT` 0) . toCodeIndent'
class IsCode c => Codegen a c where
  toCode :: a -> c

-- Basic types
instance IsCode code => Codegen Bool code where
  toCode x = if x then "true" else "false"

instance IsCode code => Codegen CString code where
  toCode = fromString . show

instance IsCode code => Codegen Integer code where
  toCode = fromString . show

instance IsCode c => Codegen Int c where
  toCode = fromString . show

instance IsCode code => Codegen Double code where
  toCode = fromString . show

instance (IsCode code, Codegen a code) => Codegen (Maybe a) code where
  toCode = maybe "Nothing()" $ \x -> "Just<" <> toCode x <> ">()g"


type Record = LiteralCode
type SymbolCode = LiteralCode
type SoftFilePath = SoftUNameCode
newtype SymbolId = SymbolId { fromSymbolId :: CString } deriving (Show, Eq)
instance IsString SymbolId where
  fromString = SymbolId

instance IsCode a => Codegen SymbolId a where
  toCode (SymbolId x) = fromString $ x >>= norm where
    -- Escape underscores as if they were backslashes.
    norm c | isAlphaNum c = [c]
           | c == '_' = "__"
           | otherwise = "_" <> show (ord c) <> "_"

instance Monoid SymbolId where
  mempty = ""
instance Semigroup SymbolId where
  x <> y = SymbolId $ fromSymbolId x <> fromSymbolId y

-- instance (Monoid a, IsString a, Codegen x a, Codegen UEOp a, Codegen BEOp a) =>
--          Codegen (Expr x) a where
--   toCode (E2 EOr (E2 EAnd ifE thenE) elseE) =
--     "("<> toCode ifE <> " ? " <> toCode thenE <> " : " <> toCode elseE <> ")"
--   toCode (E2 ELike e1 e2) = toCode $ R2 RLike (R0 e1) (R0 e2)
--   toCode (E2 op e1 e2) = toCode3 op e1 e2
--   toCode (E1 op e) = toCode2 op e
--   toCode (E0 x) = toCode x

-- instance (Monoid a, IsString a, Codegen BPOp a, Codegen UPOp a, Codegen x a) =>
--          Codegen (Prop x) a where
--   toCode (P2 op e1 e2) = toCode3 op e1 e2
--   toCode (P1 op e)     = toCode2 op e
--   toCode (P0 x)        = toCode x

-- instance (Monoid a, IsString a, Codegen BROp a, Codegen x a) =>
--          Codegen (Rel x) a where
--   toCode (R2 RLike e1 e2)      = toCodeFn' (fromString "greedy_like") [e1, e2]
--   toCode (R2 RSubstring e1 e2) = toCodeFn RSubstring [e1, e2]
--   toCode (R2 op e1 e2)         = toCode3 op e1 e2
--   toCode (R0 x)                = toCode x
-- instance IsCode code =>  Codegen ExpTypeSym code where
--   toCode = \case
--     ECount Nothing -> error "`count(*)` should have been converted to sum(1). Report this."
--     ECount (Just _) -> error "`count(distinct a)` should have been converted to count_dictinct(a). Report this."
--     ESym x -> toCode $ SymbolId x
--     EBool x -> toCode x
--     EFloat x -> toCode x
--     EString x -> "ASARR(" <> fromString (show $ length x)
--                 <> "," <> toCode x <> ")"
--     EInt x -> toCode x
--     EDate d -> toCode $ dateToInteger d
--     EInterval _ ->
--       error "Date intervals should have been squashed into dates at preproc."
-- toCode3 :: (IsString a, Monoid a, Codegen x a, Codegen y a, Codegen z a) =>
--           x -> y -> z -> a
-- toCode3 o a b = "(" <> toCode a <> " " <> toCode o <> " " <> toCode b <> ")"
-- toCode2 :: (IsString a, Monoid a, Codegen x a, Codegen y a) => x -> y -> a
-- toCode2 o x = toCodeFn o [x]
-- toCodeFn :: (IsString a, Monoid a, Codegen x a, Codegen y a) => x -> [y] -> a
-- toCodeFn o xs = toCode o <> "(" <> commaSeparate (toCode <$> xs) <> ")"
-- toCodeFn' :: (IsString a, Monoid a, Codegen y a) => a -> [y] -> a
-- toCodeFn' o xs = o <> "(" <> commaSeparate (toCode <$> xs) <> ")"
