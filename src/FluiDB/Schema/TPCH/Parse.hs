{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
module FluiDB.Schema.TPCH.Parse
  (parseTpchQuery) where

import           Control.Monad.Except
import           Control.Monad.State
import           Data.Codegen.Build.Types
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Parser
import           Data.Query.SQL.Types
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL
import           FluiDB.Classes
import           FluiDB.Schema.Common
import           FluiDB.Schema.TPCH.Values
import           FluiDB.Types

isInQCppConf :: SqlTypeVars e s t n =>
               QueryCppConf e s -> e -> Query e Table -> Maybe Bool
isInQCppConf cppConf e q =
  inQ (isJust . literalType cppConf) e
  =<< traverse (tableSchema cppConf . Just . DataFile . datFile) q


-- | Nothing in case of literal. This implementation is dumb.
inQ :: forall a e . Hashables1 e =>
      (e -> Bool) -> e -> Query e [(a,e)] -> Maybe Bool
inQ isLit e q = if isLit e then Nothing else Just $ recur q
  where
    recur :: Query e [(a,e)] -> Bool
    recur = \case
      Q0 s -> any2 (== e) s
      Q1 (QProj p) _    -> elem e $ fst <$> p
      Q1 (QGroup p _) _ -> elem e $ fst <$> p
      Q1 _ l -> recur l
      Q2 o l r  -> case o of
        QProd            -> recur l || recur r
        QJoin _          -> recur l || recur r
        QUnion           -> recur l
        QDistinct        -> recur r
        QProjQuery       -> recur l
        QLeftAntijoin _  -> recur l
        QRightAntijoin _ -> recur r


-- | Parse, expose unique columns and transform e into PlanSyms
parseTpchQuery :: forall e s t n m .
                 (MonadFakeIO m,SqlTypeVars e s t n) =>
                 String
               -> GlobalSolveT e s t n m (Query (PlanSym e s) (QueryPlan e s,s))
parseTpchQuery qtext = do
  cppConf :: QueryCppConf e s <- gets globalQueryCppConf
  (>>= either throwError return . annotateQuery cppConf)
    $ errLift
    $ eitherToExcept
    $ fmap2 (Just . DataFile . datFile)
    $ parseSQL (isInQCppConf cppConf) qtext
