{-# LANGUAGE DeriveFunctor    #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards  #-}
module Data.CppAst.LiteralCode.SoftUNameCode
  ( SoftUNameCode'(..)
  , SoftUNameCode
  , commitUNames'
  , uniquifyName
  , uniquifyName'
  , newSoftName
  , literalCodeToString
  ) where

import           Data.CppAst.LiteralCode.HasUniqueNames
import           Data.CppAst.LiteralCode.LiteralCode
import           Data.List
import qualified Data.Map                               as DM
import           Text.Printf

-- | A staging area for the unique names in order to build literal
-- code that uses the same name. Basically equivalent to the Writer
-- monad but that might be confusing to the purpose.
data SoftUNameCode' a = SoftUNameCode' {
  softUNames      :: [(Index, Id, Id -> CString)],
  hardLiteralCode :: a}
  deriving Functor

instance Applicative SoftUNameCode' where
  f <*> a = SoftUNameCode' (softUNames f <> softUNames a) $
    hardLiteralCode f $ hardLiteralCode a
  pure = SoftUNameCode' mempty

instance Monad SoftUNameCode' where
  a >>= f = join0 $ fmap f a where
    join0 (SoftUNameCode' xs (SoftUNameCode' xs' x)) =
      SoftUNameCode' (xs <> xs') x


type SoftUNameCode = SoftUNameCode' LiteralCode

instance (HasUniqueNames a, Monoid a) => Monoid (SoftUNameCode' a) where
  mempty = SoftUNameCode' mempty mempty
instance (HasUniqueNames a, Monoid a) => Semigroup (SoftUNameCode' a) where
  (SoftUNameCode' softVars lc) <> (SoftUNameCode' softVars' lc') =
    SoftUNameCode' (softVars <> fmap rebaseIndex softVars') (lc <> lc') where
    rebaseIndex (index, b, c) = (index + maxNameIndex lc, b, c)

-- | Get both a functor of variables and the literal code. This is not
-- available for multiple layers of SoftUNameCode' to avoid conflicts.
commitUNames' :: Functor f =>
                f Int -> SoftUNameCode -> (f (Maybe LiteralCode), LiteralCode)
commitUNames' vars (SoftUNameCode' softUNames lc@LiteralCode{..}) = let
  appNames = (\(index, i, mk) -> (index, i + nextHardId, mk)) <$> softUNames
  nameMap = DM.fromList $ zip [sid | (_, sid, _) <- softUNames] appNames
  hardUNamesNew = appNames ++ hardUNames
  standaloneUname (_, i, mk) = mempty{hardUNames=[(0, i, mk)], nextHardId=i+1}
  in
    (fmap standaloneUname . (`DM.lookup` nameMap) <$> vars,
     lc{hardUNames=hardUNamesNew,
        nextHardId = 1 + maximum (0:[x | (_, x, _) <- hardUNamesNew])})

instance HasUniqueNames a => HasUniqueNames (SoftUNameCode' a) where
  maxNameIndex = maxNameIndex . hardLiteralCode

uniquifyName' :: Int -> (Id -> String) -> SoftUNameCode
uniquifyName' i f = mempty{softUNames=[(0, i, f)]}

uniquifyName :: Int -> String -> SoftUNameCode
uniquifyName i n = uniquifyName' i $ (n ++) . show

-- | In some cases we are making only a part of the soft name code
-- using variables passed in from other parts of the code. In that
-- case there is a danger of conflicting variable ids. This function
-- creates a variable that is guaranteed not to conflict with the
-- input code.
newSoftName :: SoftUNameCode -> (Id -> String) -> SoftUNameCode
newSoftName sc = uniquifyName' $
  maximum (0:[i + 1 | (_, i, _) <- softUNames sc])

literalCodeToString :: LiteralCode -> String
literalCodeToString LiteralCode{..} =
  buildBody 0 fromLiteralCode $ sortOn skey hardUNames where
  skey (a, _, _) = a
  buildBody _ x [] = x
  buildBody i [] vs = mconcat $ f <$> vs where
    f (i', n, mk) = if i == i'
                    then mk n
                    else error $ printf "End of string: %d, var #%d index %d" i n i'
  buildBody i (x:xs) vars@((i', n, mk):rest) =
    if i == i'
    then mk n ++ buildBody i (x:xs) rest
    else x:buildBody (i+1) xs vars
