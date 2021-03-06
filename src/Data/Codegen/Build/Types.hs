{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Data.Codegen.Build.Types
  (QueryShape
  ,PrimKeys
  ,StringSymbol
  ,LiteralType
  ,ColumnType
  ,Uniquified
  ,ScopeEnv(..)
  ,CodeBuildErr(..)
  ,CBState(..)
  ,SizeInferenceError(..)
  ,QueryCppConf(..)) where

import qualified Data.CppAst                  as CC
import qualified Data.HashSet                 as HS
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.QFile
import           Data.String
import           Data.Utils.AShow
import           GHC.Generics

data SizeInferenceError e s t n
  = NoQnf (NodeRef n) [NodeRef n]
  | EmptyQnfList (NodeRef n)
  | UnsupportedBinOp (BQOp e)
  | UnsupportedUnOp (UQOp e)
  | SIERichMsg (AShowStr e s)
    -- | UnsizedBottomNode [AnyCluster e s t n] (Query e s) (NodeRef n) (RefMap n Bool)
  | OversizedRow (QNFQuery e s)
  | SIEAShowMsg (AShowStr e s)
    -- The plan enumerator called the non-empty
    -- version of the function rather than the
    -- empty one.
  | NoPlansProvided
  deriving (Eq,Generic)
deriving instance (Show e, Show s, AShowV e, AShowV s) =>
  Show (SizeInferenceError e s t n)
instance (AShowV e, AShowV s) => AShow (SizeInferenceError e s t n)
instance AShowError e s (SizeInferenceError e s t n)

type ClustIO n = ([NodeRef n], [NodeRef n])
newtype ScopeEnv t a = ScopeEnv {runScopeEnv :: t (a, CC.Symbol CC.CodeSymbol)}
data CodeBuildErr e s t n
  = CBESizeInferenceError (SizeInferenceError e s t n)
  | MissingInputFile (ClustIO n)
    -- | We found a transition on t but it doesn't correspond to a
    -- cluster.
  | NoClusterForT (NodeRef t)
    -- | Tried to delete node n but couldn't find corresponding cluster
    -- to check if it's an intermediate.
  | NoClusterForN (NodeRef n)
    -- Generic error for not found node n
  | NodeNotFoundN (NodeRef n)
    -- The epoch attempts to overwrite an already cache query.
  | OverwritingFile (NodeRef n) QFile (Query e s)
    -- Union types are not equal
  | UnionTypeError (CppSchema' (ShapeSym e s)) (CppSchema' (ShapeSym e s))
    -- A forward transition created a symbol. Only reverse transitions
    -- can create symvbols.
  | ForwardCreateSymbol (NodeRef n)
    -- Types on either side of an equality in a join do not match.
  | BinOpTypeMismatch String (Expr CC.CppType) (Expr CC.CppType)
    -- The symbol does not correspond to the queries.
  | InvalidSymbol Int (ShapeSym e s)
    -- Array is too short for the operation (TooShortArray minLen len expr)
  | TooShortArray Int Int (Expr CC.CppType)
    -- Expected an array but got type
  | ExpectedArray CC.CppType (Expr CC.CppType)
    -- We found nested complements
  | NestedComplementsCodegen
    -- Complement expression can only be created as byproducts.
  | ComplementCode
    -- In a code block all symbol definitions need to have unique names.x
  | DuplicateSymbols [CC.CodeSymbol]
    -- Misc (for printing unshowable stuff)
  | Misc (QueryShape e s,[Expr (ShapeSym e s)])
         (QueryShape e s,[Expr (ShapeSym e s)])
    -- This is a bug where there is a circular reference between
    -- classes.
  | CircularReference String
    -- We use scopes to create the arguments of various functions. Until
    -- we make this typesafe the arguments are a list. This is thrown
    -- when there. If you see this it's a bug in that the caller
    -- provides too many arguments for the callee.
    --
    -- XXX: the way to fix this is to have ScopeEnv be a GADT (or some
    -- other dependent type).
  | WrongNumQueriesInScope Int [QueryShape e s]
    -- This is thrown in places that should not have been reached.
  | Unreachable
    -- All unique must always be projected through the projections.
  | UniqueKeyNotProjected (ShapeSym e s) (QueryShape e s)
    -- Expected a symbol but this is a literal.
  | ExpectedSymbol (ShapeSym e s)
    -- Tried to reverse a symbol (rather than an operator).
  | CantFindScore String (NodeRef n) [NodeRef n]
  | BuildErrMsg String
  | BuildErrAShow (AShowStr e s)
  | UnknownBuildError
  deriving (Eq,Generic)
instance (AShowV e, AShowV s) => AShow (CodeBuildErr e s t n)
instance AShowError e s (CodeBuildErr e s t n)

instance IsString (CodeBuildErr e s t n) where fromString = BuildErrMsg

-- | Code builder state. All the stuff outside main() that main()
-- depends on.
data CBState e s t n =
  CBState
  { -- XXX: move this to a reader.
    cbQueryCppConf   :: QueryCppConf e s
   ,cbMatNodePlans   :: RefMap n (QueryShape e s)
   ,cbIncludes       :: HS.HashSet CC.Include
   ,cbClasses        :: CC.CodeCache CC.Class
   ,cbFunctions      :: CC.CodeCache CC.Function
   ,cbQueryFileCache :: QueryFileCache e s
   ,cbDataDir        :: FilePath
  }

type PrimKeys e = [e]
type StringSymbol = Maybe String
type LiteralType = CC.CppType
type ColumnType = CC.CppType
type Uniquified e = e

data QueryCppConf e s =
  QueryCppConf
  { literalType           :: e -> Maybe LiteralType
   ,tableSchema           :: s -> Maybe (CppSchema' e)
   ,columnType            :: e -> s -> Maybe ColumnType
    -- |The c++ variables we are going to make.
   ,toSymbol              :: e -> StringSymbol
   ,defaultQueryFileCache :: QueryFileCache e s
   ,uniqueColumns         :: s -> Maybe (PrimKeys e)
   ,asUnique              :: Int -> e -> Maybe (Uniquified e)
   ,dataDir               :: FilePath
  }
