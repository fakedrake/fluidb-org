{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.FluiDB.Running.ParseTpch
  (parseTpchQuery
  , putPlanSymTpch
  ) where

import Database.FluiDB.ListT
import Database.FluiDB.CnfQuery
import Database.FluiDB.Default
import Database.FluiDB.Codegen.Schema
import Data.Bitraversable
import Data.Bifunctor
import Database.FluiDB.Optimizations.ExposeUnique
import Database.FluiDB.AShow
import Data.Maybe
import Database.FluiDB.Relational.Parser.Types
import Control.Monad.State
import Database.FluiDB.Relational.Parser.NewParser
import Database.FluiDB.FileSet
import Database.FluiDB.Running.TpchValues
import Database.FluiDB.Utils
import Control.Monad.Except
import Database.FluiDB.QuerySchema
import Database.FluiDB.Algebra
import Database.FluiDB.Running.Types
import Database.FluiDB.Codegen.Build.Types
import Database.FluiDB.Running.Classes

isInQCppConf :: SqlTypeVars e s t n =>
               QueryCppConf e s -> e -> Query e Table -> Maybe Bool
isInQCppConf cppConf e q = inQ (isJust . literalType cppConf) e
  =<< traverse (tableSchema cppConf . Just . DataFile . datFile) q


-- | Put unique columns and transform e into plansyms.
putPlanSymTpch :: Hashables2 e s =>
                 QueryCppConf e s
               -> Query e s
               -> Either (GlobalError e s t n) (Query (PlanSym e s) (QueryPlan e s,s))
putPlanSymTpch cppConf q = do
  let uniqSym = \e -> do
        i <- get
        case asUnique cppConf i e of
          Just e' -> modify (+1) >> return e'
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
    $ fmap (bitraverse
            (pure . uncurry mkPlanSym)
            (\s -> (,s) <$> mkPlanFromTbl cppConf s)
            . snd . fromJustErr)
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
  cppConf :: QueryCppConf e s <- globalQueryCppConf <$> get
  (>>= either throwError return . putPlanSymTpch cppConf)
    $ errLift
    $ eitherToExcept
    $ fmap2 (Just . DataFile . datFile)
    $ parseSQL (isInQCppConf cppConf) qtext
