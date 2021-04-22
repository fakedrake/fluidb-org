{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Data.CppAst.Class
  ( Class(..)
  , classNameRef
  ) where
import           Data.CppAst.CodeSymbol
import           Data.CppAst.Constructor
import           Data.CppAst.Declaration
import           Data.CppAst.Function
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Member
import           Data.CppAst.Symbol
import           Data.CppAst.TypeDef
import           Data.List
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics                    (Generic)

data Class a = Class {
  className            :: Symbol a,
  classConstructors    :: [Constructor a],
  classPublicFunctions :: [Function a],
  classPublicMembers   :: [Declaration a],
  classTypeDefs        :: [TypeDef a],
  classPrivateMembers  :: [Member a]
  } deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
classNameRef :: Class CodeSymbol -> Symbol CodeSymbol
classNameRef = fmap symbolRef . className
instance Hashable a => Hashable (Class a)
instance AShow a => AShow (Class a)
instance (IsCode c, Codegen a c) => CodegenIndent (Class a) c where
  toCodeIndent' Class{..} = do
    putLine $ "class " <> toCode className <> " {"
    putLine " public:"
    withOffset 2 $ do
      mapM_ toCodeIndent'' classConstructors
      mapM_ toCodeIndent' classPublicFunctions
      mapM_ toCodeIndent' classPublicMembers
      mapM_ toCodeIndent' classTypeDefs
    putLine " private:"
    withOffset 2 $ mapM_ toCodeIndent' classPrivateMembers

    putLine "};"
    where
      -- We need access to the class, that's why it's not it's own
      -- instnace.
      toCodeIndent'' Constructor{..} = do
        putLine $ toCode className
          <> "("
          <> mconcat (intersperse ", " $ toCode <$> constructorArguments)
          <> ") "
          <> if null initMembers
            then ""
            else ": " <> mconcat (intersperse ", " initMembers)
        putLine "{"
        withOffset 2 $ mapM_ toCodeIndent' constructorBody
        putLine "}"
        where
          initMembers = (`map` constructorMemberConstructors) $ \(sym, ini) ->
           toCode sym <> "(" <> toCode ini <> ")"
