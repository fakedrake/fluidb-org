{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
module FluiDB.Schema.TPCH.Parse
  (parseTpchQuery
  , putPlanSymTpch
  ) where

import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.CnfQuery.Build
import           Data.Codegen.Build.Types
import           Data.Codegen.Schema
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Query.Optimizations.ExposeUnique
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Parser
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.MTL
import           Data.Utils.Unsafe
import           FluiDB.Classes
import           FluiDB.Schema.TPCH.Values
import           FluiDB.Types

isInQCppConf :: SqlTypeVars e s t n =>
               QueryCppConf e s -> e -> Query e Table -> Maybe Bool
isInQCppConf cppConf e q =
  inQ (isJust . literalType cppConf) e
  =<< traverse (tableSchema cppConf . Just . DataFile . datFile) q

-- | Put unique columns and transform e into plansyms.
putPlanSymTpch
  :: Hashables2 e s
  => QueryCppConf e s
  -> Query e s
  -> Either (GlobalError e s t n) (Query (PlanSym e s) (QueryPlan e s,s))
putPlanSymTpch cppConf q = do
  let uniqSym = \e -> do
        i <- get
        case asUnique cppConf i e of
          Just e' -> modify (+ 1) >> return e'
          Nothing -> throwAStr $ "Not a symbol: " ++ ashow e
  qUniqExposed <- maybe
    (throwAStr "Couldn't find uniq:")
    ((>>= maybe (throwAStr "Couldn't expose uniques") (return . fst))
     . headListT
     . (`evalStateT` (0 :: Int))
     . exposeUnique uniqSym)
    $ traverse (\s -> (s,) <$> uniqueColumns cppConf s) q
  first toGlobalError
    $ (>>= maybe (throwAStr "Unknown symbol") return)
    $ fmap
      (bitraverse
         (pure . uncurry mkPlanSym)
         (\s -> (,s) <$> mkPlanFromTbl cppConf s)
       . snd
       . fromJustErr)
    $ (`evalStateT` def)
    $ headListT
    $ toCNF (fmap2 snd . tableSchema cppConf) qUniqExposed

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
  (>>= either throwError return . putPlanSymTpch cppConf)
    $ errLift
    $ eitherToExcept
    $ fmap2 (Just . DataFile . datFile)
    $ parseSQL (isInQCppConf cppConf) qtext
