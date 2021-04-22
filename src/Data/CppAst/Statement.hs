{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.Statement
  ( Statement(..)
  , coutSt
  , constString
  ) where

import           Data.CppAst.Assignment
import           Data.CppAst.Declaration
import           Data.CppAst.Expression
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Symbol
import           Data.CppAst.TypeModifier
import           Data.List
import qualified Data.Set                        as DS
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

data Statement a
  = DeclarationSt (Declaration a) (Maybe (Expression a))
  | AssignmentSt (Assignment a)
  | ExpressionSt (Expression a)
  | ReturnSt (Expression a)
  | IfBlock (Expression a) [Statement a]
  | WhileBlock (Expression a) [Statement a]
  | IfElseBlock (Expression a) [Statement a] [Statement a]
  | ForBlock (Assignment a,Expression a,Assignment a) [Statement a]
  | ForEachBlock (Declaration a,Expression a) [Statement a]
  | Block [Statement a]
  | Comment String
  | StreamSt (Symbol a) [Expression a]
  deriving (Eq,Show,Functor,Foldable,Traversable,Generic)
coutSt :: IsString a => [Expression a] -> Statement a
coutSt xs = StreamSt "std::cout" $ xs ++ ["std::endl"]
instance Hashable a => Hashable (Statement a)
instance AShow a => AShow (Statement a)
constString :: IsString a => Type a
constString = ClassType (DS.singleton CppConst) [] "std::string"

splitLines :: String -> [String]
splitLines = go []
  where
    go cur = \case
      [] -> []
      '\n':xs -> cur : go [] xs
      x:xs -> go (cur ++ [x]) xs

putLinesIndented :: Codegen a c => [Statement a] -> Offset c ()
putLinesIndented body = withOffset 2 $ mapM_ toCodeIndent' body
instance Codegen a c => CodegenIndent (Statement a) c where
  toCodeIndent' = \case
    Comment str -> mapM_ (putLine . fromString) commentLines where
      commentLines =  ("// " ++) <$> splitLines str
    DeclarationSt decl Nothing -> putLine $ toCode decl <> ";"
    DeclarationSt decl (Just e) -> putLine $ toCode decl <> " = " <> toCode e <> ";"
    AssignmentSt ass -> putLine $ toCode ass <> ";"
    ExpressionSt e -> putLine $ toCode e <> ";"
    ReturnSt e -> putLine $ "return " <> toCode e <> ";"
    IfBlock e sts -> do
      putLine $ "if (" <> toCode e <> ") {"
      putLinesIndented sts
      putLine "}"
    WhileBlock e sts -> do
      putLine $ "while (" <> toCode e <> ") {"
      putLinesIndented sts
      putLine "}"
    Block sts -> do
      putLine "{"
      putLinesIndented sts
      putLine "}"
    IfElseBlock e sts ests -> do
      putLine $ "if (" <> toCode e <> ") {"
      putLinesIndented sts
      putLine "} else {"
      putLinesIndented ests
      putLine "}"
    ForBlock (ass, br, step) sts -> do
      putLine $ "for ("
        <> toCode ass <> "; "
        <> toCode br <> "; "
        <> toCode step
        <> ") {"
      putLinesIndented sts
      putLine "}"
    ForEachBlock (decl, list) sts -> do
      putLine $ "for ("
        <> toCode decl <> ": "
        <> toCode list
        <> ") {"
      putLinesIndented sts
      putLine "}"
    StreamSt c exps -> putLine
      $ toCode c <> " << "
      <> mconcat (intersperse " << " $ toCode <$> exps)
      <> ";"
