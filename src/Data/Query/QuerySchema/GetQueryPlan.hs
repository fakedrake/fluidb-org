{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}

module Data.Query.QuerySchema.GetQueryPlan
  ( exprCppType'
  , exprCppType
  , planSymType
  , getSymPlan
  , getQueryPlanPrj
  , getQueryPlanGrp
  , exprColumnProps
  , QueryPlanError
  ) where

import           Control.Monad.Except
import           Control.Monad.Extra
import           Data.Bifunctor
import           Data.Codegen.Build.Types
import           Data.CppAst                       as CC
import qualified Data.List.NonEmpty                as NEL
import           Data.Maybe
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
import           Data.Utils.AShow
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           GHC.Generics                      (Generic)

exprCppTypeBinGeneric
  :: BEOp -> Expr CC.CppType -> Expr CC.CppType -> Maybe CC.CppType
exprCppTypeBinGeneric o l r = do
  lTy <- exprCppType' l
  rTy <- exprCppType' r
  if lTy == rTy then return lTy else case (o, lTy, rTy) of
    (EAdd, CC.CppArray t s, CC.CppArray t' s') -> if t /= t'
      then Nothing
      else return $ CC.CppArray t $ s `addSize` s'
    _ -> if isNumericOp o
        then do (pl',pr') <- (,) <$> prec lTy <*> prec rTy
                Just $ if pl' > pr' then lTy else rTy
        else Nothing
  where
    isNumericOp = \case
      EAdd -> True
      ESub -> True
      EMul -> True
      EDiv -> True
      _    -> False
    prec :: CppTypeF c -> Maybe Integer
    prec = \case
      CC.CppNat    -> Just 1
      CC.CppInt    -> Just 2
      CC.CppDouble -> Just 3
      _            -> Nothing
    addSize = curry $ \case
      (CC.LiteralSize x, CC.LiteralSize y) -> CC.LiteralSize $ x + y
      (x, y) -> CC.SizeOf $ CC.E2ession "+" (toExpre x) $ toExpre y where
        toExpre = \case
          CC.SizeOf x'      -> x'
          CC.LiteralSize x' -> CC.LiteralIntExpression x'

-- Array type of length i and throw error if it's the argument
-- expression length is known and larger than minLen.
setArrLen :: Expr CC.CppType -> Maybe Int -> Int -> Maybe CC.CppType
setArrLen e minLen i = do
  let retArr t = return $ CC.CppArray t $ CC.LiteralSize i
  exprCppType' e >>= \case
    CC.CppArray t (CC.SizeOf _) -> retArr t -- c compiler will check.
    CC.CppArray t (CC.LiteralSize l) -> case minLen of
      Just ml -> if l < ml
                then Nothing
                else return $ CC.CppArray t $ CC.LiteralSize i
      Nothing -> retArr t
    _ -> Nothing

-- | Return the type of a symbol
planSymType :: Hashables2 e s =>
              (e -> Maybe CC.CppType)
            -> [QueryPlan e s]
            -> PlanSym e s
            -> Maybe CC.CppType
planSymType literalType =
  (>>= either literalType pure) ... planSymTypeSym'

aggrCppType :: Aggr CC.CppType -> CC.CppType
aggrCppType (NAggr fn t) = case fn of
  AggrSum   -> t
  AggrCount -> CC.CppNat
  AggrAvg   -> t
  AggrMin   -> t
  AggrMax   -> t
  AggrFirst -> t

exprCppType' :: Expr CC.CppType -> Maybe CC.CppType
exprCppType' = \case
  E2 o l r -> case o of
    EEq   -> boolT
    ELike -> boolT
    ENEq  -> boolT
    -- (Bool && Int) || Int
    EOr   -> exprCppType' r
    EAnd  -> exprCppType' r
    _     -> exprCppTypeBinGeneric o l r
  E1 o e -> case o of
    EAbs -> exprCppType' e
    ESig -> exprCppType' e
    ENot -> boolT
    ENeg -> exprCppType' e
    -- Assume all functions are endofunctors
    EFun o' -> case o' of
      ExtractDay     -> return CC.CppInt
      ExtractMonth   -> return CC.CppInt
      ExtractYear    -> return CC.CppInt
      Prefix i       -> setArrLen e (Just i) i
      Suffix i       -> setArrLen e (Just i) i
      SubSeq i j     -> setArrLen e (Just j) $ j - i
      AssertLength i -> setArrLen e Nothing i
  E0 e -> return e
-- | Add CppSizes
  where
    boolT = return CC.CppBool

exprColumnProps
  :: Hashables2 e s
  => (e -> Maybe CC.CppType)
  -> [QueryPlan e s]
  -> Expr (Either CppType (PlanSym e s))
  -> Maybe ColumnProps
exprColumnProps litType plans expr = do
  ty <- exprCppType litType plans expr
  cnst <- either (const Nothing) return
    $ allM
      (either (const $ return True) (\s -> anyM (`planSymConst` s) plans))
    $ toList expr
  return
    ColumnProps
    { columnPropsCppType = ty
     ,columnPropsConst = cnst
    }


exprCppType :: Hashables2 e s =>
              (e -> Maybe CC.CppType)
            -> [QueryPlan e s]
            -> Expr (Either CppType (PlanSym e s))
            -> Maybe CC.CppType
exprCppType litType plans = exprCppType'
  <=< traverse (either Just $ planSymType litType plans)
data QueryPlanError e s
  = QPEErrorMsg (AShowStr e s)
  | QPEExpType (Expr (CppType,Maybe e))
  | QPEFailedColProp [e] e
  | QPEUnknown Int
  deriving Generic
instance AShowError e s (QueryPlanError e s) where
instance (AShowV e, AShowV s) => AShow (QueryPlanError e s)

exprCppType'' :: Expr (CppType,Maybe e) -> Either (QueryPlanError e s) CppType
exprCppType'' expt =
  maybe (Left $ QPEExpType expt) Right $ exprCppType' $ fst <$> expt

planSymConst
  :: (HasCallStack,Hashables2 e s)
  => QueryPlan e s
  -> PlanSym e s
  -> Either (QueryPlanError e s) Bool
planSymConst QueryPlan{qpSchema=sch} sym = case planSymQnfName sym of
  NonSymbolName _ -> return True
  _ -> maybe (throwAStr $ "oops: " ++ ashow (sym,sch)) (return . columnPropsConst)
    $ lookup sym sch

planSymType'
  :: Hashables2 e s
  => (e -> Maybe CppType)
  -> [QueryPlan e s]
  -> PlanSym e s
  -> Either (QueryPlanError e s) CppType
planSymType' litTy plans sym = case planSymType litTy plans sym of
  Nothing -> throwAStr $ "Couldn't get type of sym: " ++ ashow (sym,plans)
  Just x  -> return x

aggrType
  :: Hashables2 e s
  => (e -> Maybe CppType)
  -> QueryPlan e s
  -> Aggr (Expr (PlanSym e s))
  -> Either (QueryPlanError e s) CppType
aggrType litType plan = fmap aggrCppType
  . traverse expType
  where

    expType expr = do
      exp1 <- traverse annotate expr
      exprCppType'' exp1
        where
          annotate e = (,Just $ planSymOrig e)
            <$> planSymType' litType [plan] e


-- The cases are:
-- * QGroup [(a,..),(b,..)...] [] -> [[a],[b],..]
-- * QGroup [(a,..),(b,..)...] [k1,k2..] -> [[a],[b],..]
getQueryPlanGrp :: forall e s . (HasCallStack,Hashables2 e s) =>
                  (e -> Maybe CppType)
                -> [(PlanSym e s,Expr (Aggr (Expr (PlanSym e s))))]
                -> [Expr (PlanSym e s)]
                -> QueryPlan e s
                -> Either (QueryPlanError e s) (QueryPlan e s)
getQueryPlanGrp litType proj es qplan = do
  sch <- projQueryPlanInternal' =<< traverse3 aexpProps proj
  case (es,NEL.nonEmpty proj) of
    (_,Nothing) -> throwAStr $ "Empty group projection: " ++ ashow (proj,es)
    ([],Just nelProj) -> return
      QueryPlan
      { qpSchema = [(e,p { columnPropsConst = True }) | (e,p) <- sch]
       ,qpUnique = return . fst <$> nelProj
      }
    _ -> do
      let uniqCandidates = case grpSymsM of
            Nothing -> qpUnique qplan
            Just x  -> deoverlap $ return x <> qpUnique qplan
      case mapMaybeNEL (traverse (`lookup` ioAssoc)) uniqCandidates of
        Left _ -> throwAStr
          $ "LU fail: "
          ++ ashow (proj,ioAssoc,toList <$> toList uniqCandidates)
        Right
          uniq -> return QueryPlan { qpSchema = sch,qpUnique = deoverlap uniq }
  where
    deoverlap =
      go
      . fmap snd
      . NEL.sortBy (\(x,_) (y,_) -> compare x y)
      . fmap (\x -> (length x,x))
      where
        go (x NEL.:| xs) =
          (x NEL.:|)
          $ maybe [] (toList . go)
          $ NEL.nonEmpty
          $ filter (not . isSubset x) xs
        isSubset x y = all (`elem` y) x
    grpSymsM =
      traverse (\case
                  E0 e -> Just e
                  _    -> Nothing) =<< NEL.nonEmpty es
    ioAssoc :: [(PlanSym e s,PlanSym e s)]
    ioAssoc = mapMaybe (\case
                          (o,E0 (NAggr AggrFirst (E0 i))) -> Just (i,o)
                          _                               -> Nothing) proj
    aexpProps :: Aggr (Expr (PlanSym e s))
              -> Either (QueryPlanError e s) ColumnProps
    aexpProps agg = do
      ty <- aggrType litType qplan agg
      cnst <- allM (planSymConst qplan) $ toList2 agg
      return ColumnProps { columnPropsCppType = ty,columnPropsConst = cnst }

mapMaybeNEL :: HasCallStack =>
              (NEL.NonEmpty a -> Maybe (NEL.NonEmpty b))
            -> NEL.NonEmpty (NEL.NonEmpty a)
            -> Either (QueryPlanError e s) (NEL.NonEmpty (NEL.NonEmpty b))
mapMaybeNEL f =
  maybe (throwAStr "oops") return
  -- ALL the subexp should be translatable (not realistic FIXME)
  . NEL.nonEmpty
  . mapMaybe f
  . toList

getQueryPlanPrj :: forall e s . (HasCallStack,Hashables2 e s) =>
                  (e -> Maybe CppType)
                -> [(PlanSym e s,Expr (PlanSym e s))]
                -> QueryPlan e s
                -> Either (QueryPlanError e s) (QueryPlan e s)
getQueryPlanPrj litType proj qplan = do
  sch <- projQueryPlanInternal' =<< traverse3 go proj
  case mapMaybeNEL (traverse (`lookup` ioAssoc)) (qpUnique qplan) of
    Left _ -> throwAStr
      $ "No unique sets exposed in their entirety: "
      ++ ashow (toList <$> toList (qpUnique qplan),ioAssoc,qpSchema qplan,proj)
    Right uniq -> return
      QueryPlan
      { qpSchema = sch
       ,qpUnique = uniq
      }
  where
    ioAssoc = mapMaybe (\case
                          (o,E0 i) -> Just (i,o)
                          _        -> Nothing) proj
    go :: PlanSym e s -> Either (QueryPlanError e s) ColumnProps
    go e = do
      ty <- planSymType' litType [qplan] e
      cnst <- planSymConst qplan e
      return
        ColumnProps
        { columnPropsCppType = ty
         ,columnPropsConst = cnst
        }

projQueryPlanInternal'
  :: Hashables2 e s
  => [(PlanSym e s,Expr ColumnProps)]
  -> Either (QueryPlanError e s) [(PlanSym e s,ColumnProps)]
projQueryPlanInternal' proj = forM proj $ \(sym,expt) -> do
  ty <- exprCppType'' $ (,Nothing) . columnPropsCppType <$> expt
  return
    (sym
    ,ColumnProps
       { columnPropsCppType = ty
        ,columnPropsConst = all columnPropsConst expt
       })

getSymPlan
  :: forall er e s m .
  (Hashables2 e s,AShowError e s er,MonadError er m)
  => (s -> Maybe [e])
  -> (s -> Maybe (CppSchema' e))
  -> s
  -> m (QueryPlan e s)
getSymPlan prims symSchema s = do
  sch :: CppSchema' e <- maybe (throwAStr "No schema for table") return
    $ symSchema s
  schAnnotUniq :: [(CppType,(PlanSym e s,Bool))] <- traverse2 makeSym sch
  when (null schAnnotUniq) $ throwAStr $ "Table has empty schema: " ++ ashow s
  unless (any (snd . snd) schAnnotUniq)
    $ throwAStr
    $ "No unique columns: "
    ++ ashow (s,first planSymQnfOriginal . snd <$> schAnnotUniq)
  maybe (throwAStr $ "No unique columns: " ++ ashow s) return
    $ mkQueryPlan schAnnotUniq
  where
    makeSym :: Monad m => e -> m (PlanSym e s,Bool)
    makeSym e = do
      pks <- maybe (throwAStr "Missing primkeys") return $ prims s
      es <- maybe (throwAStr "Missing primkeys") return
        $ fmap2 snd
        $ symSchema s
      let isPrim = e `elem` pks
          (nm,_) = nqnfSymbol es s
      planSym :: PlanSym e s
        <- either (const $ throwAStr "mkSymPlanSymNM failed") return
        $ mkSymPlanSymNM nm e
      return (planSym,isPrim)
