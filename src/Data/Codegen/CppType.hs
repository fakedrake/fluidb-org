{-# LANGUAGE CPP                   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeSynonymInstances  #-}

module Data.Codegen.CppType
  ( cppTypeSize
  , cppTypeAlignment
  , schemaPostPaddings
  , compatCppTypes
  ) where

import           Data.CppAst.CppType

compatCppTypes :: CppType -> CppType -> Bool
compatCppTypes t1 t2 = t1 == t2 || (isNum t1 && isNum t2) where
  isNum = \case
    CppInt    -> True
    CppDouble -> True
    CppNat    -> True
    _         -> False

cppTypeSize :: CppTypeF a -> Maybe Int
cppTypeSize = \case
  CppArray t (LiteralSize l)
    -> (* l) <$> cppTypeSize t
  CppArray _ (SizeOf _) -> Nothing
  CppVoid -> Nothing
  CppChar -> Just 1
  CppNat -> Just 4
  CppInt -> Just 4
  CppDouble -> Just 8
  CppBool -> Just 1

cppTypeAlignment :: CppTypeF a -> Maybe Int
cppTypeAlignment = \case
  CppArray t _ -> cppTypeSize t
  x            -> cppTypeSize x

schemaPostPaddings :: [CppType] -> Maybe [Int]
schemaPostPaddings [] = Just []
schemaPostPaddings [_] = Just [0]
schemaPostPaddings schema = do
  elemSizes <- sequenceA [cppTypeSize t | t <- schema]
  spaceAligns' <- sequenceA [cppTypeAlignment t | t <- schema]
  let (_:spaceAligns) = spaceAligns' ++ [maximum spaceAligns']
  let offsets = 0 : zipWith3 getOffset spaceAligns offsets elemSizes
  return $ zipWith (-) (zipWith (-) (tail offsets) offsets) elemSizes
  where
    getOffset nextAlig off size =
      (size + off)
      + ((nextAlig - ((size + off) `mod` nextAlig)) `mod` nextAlig)
