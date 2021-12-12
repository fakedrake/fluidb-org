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

module Data.Query.QuerySchema.GetQueryShape
  ( exprCppType'
  , exprCppType
  , shapeSymType
  , getSymShape
  , getQueryShapePrj
  , getQueryShapeGrp
  , exprColumnProps
  , QueryShapeError
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
import           Data.Query.QuerySize
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
shapeSymType :: Hashables2 e s =>
              (e -> Maybe CC.CppType)
            -> [QueryShape e s]
            -> ShapeSym e s
            -> Maybe CC.CppType
shapeSymType literalType =
  (>>= either literalType pure) ... shapeSymTypeSym'

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
  where
    boolT = return CC.CppBool

exprColumnProps
  :: Hashables2 e s
  => (e -> Maybe CC.CppType)
  -> [QueryShape e s]
  -> Expr (Either CppType (ShapeSym e s))
  -> Maybe ColumnProps
exprColumnProps litType shapes expr = do
  ty <- exprCppType litType shapes expr
  cnst <- either (const Nothing) return
    $ allM
      (either (const $ return True) (\s -> anyM (`shapeSymConst` s) shapes))
    $ toList expr
  return
    ColumnProps
    { columnPropsCppType = ty
     ,columnPropsConst = cnst
    }


exprCppType :: Hashables2 e s =>
              (e -> Maybe CC.CppType)
            -> [QueryShape e s]
            -> Expr (Either CppType (ShapeSym e s))
            -> Maybe CC.CppType
exprCppType litType shapes = exprCppType'
  <=< traverse (either Just $ shapeSymType litType shapes)
data QueryShapeError e s
  = QPEErrorMsg (AShowStr e s)
  | QPEExpType (Expr (CppType,Maybe e))
  | QPEFailedColProp [e] e
  | QPEUnknown Int
  deriving Generic
instance AShowError e s (QueryShapeError e s) where
instance (AShowV e, AShowV s) => AShow (QueryShapeError e s)

exprCppType'' :: Expr (CppType,Maybe e) -> Either (QueryShapeError e s) CppType
exprCppType'' expt =
  maybe (Left $ QPEExpType expt) Right $ exprCppType' $ fst <$> expt

shapeSymConst
  :: (HasCallStack,Hashables2 e s)
  => QueryShape e s
  -> ShapeSym e s
  -> Either (QueryShapeError e s) Bool
shapeSymConst QueryShape {qpSchema = sch} sym = case shapeSymQnfName sym of
  NonSymbolName _ -> return True
  _ -> maybe
    (throwAStr $ "oops: " ++ ashow (sym,sch))
    (return . columnPropsConst)
    $ lookup sym sch

shapeSymType'
  :: Hashables2 e s
  => (e -> Maybe CppType)
  -> [QueryShape e s]
  -> ShapeSym e s
  -> Either (QueryShapeError e s) CppType
shapeSymType' litTy shapes sym = case shapeSymType litTy shapes sym of
  Nothing -> throwAStr $ "Couldn't get type of sym: " ++ ashow (sym,shapes)
  Just x  -> return x

aggrType
  :: Hashables2 e s
  => (e -> Maybe CppType)
  -> QueryShape e s
  -> Aggr (Expr (ShapeSym e s))
  -> Either (QueryShapeError e s) CppType
aggrType litType shape = fmap aggrCppType
  . traverse expType
  where

    expType expr = do
      exp1 <- traverse annotate expr
      exprCppType'' exp1
        where
          annotate e = (,Just $ shapeSymOrig e)
            <$> shapeSymType' litType [shape] e



-- The cases are:
-- * QGroup [(a,..),(b,..)...] [] -> [[a],[b],..]
-- * QGroup [(a,..),(b,..)...] [k1,k2..] -> [[a],[b],..]
getQueryShapeGrp
  :: forall e s .
  (HasCallStack,Hashables2 e s)
  => (e -> Maybe CppType)
  -> [(ShapeSym e s,Expr (Aggr (Expr (ShapeSym e s))))]
  -> [Expr (ShapeSym e s)]
  -> QueryShape e s
  -> Either (QueryShapeError e s) (QueryShape e s)
getQueryShapeGrp litType proj es qshape = do
  sch <- projQueryShapeInternal' =<< traverse3 aexpProps proj
  rowSize <- getRowSize sch
  let querySize =
        QuerySize
        { qsTables = TableSize { tsRowSize = rowSize,tsRows = 10 }
         ,qsCertainty = 0.1
        }
  case (es,NEL.nonEmpty proj) of
    (_,Nothing) -> throwAStr $ "Empty group projection: " ++ ashow (proj,es)
    ([],Just nelProj) -> return
      QueryShape
      { qpSchema = [(e,p { columnPropsConst = True }) | (e,p) <- sch]
       ,qpUnique = return . fst <$> nelProj
       ,qpSize = querySize
      }
    _ -> do
      let uniqCandidates = case grpSymsM of
            Nothing -> qpUnique qshape
            Just x  -> deoverlap $ return x <> qpUnique qshape
      case mapMaybeNEL (traverse (`lookup` ioAssoc)) uniqCandidates of
        Left _ -> throwAStr
          $ "LU fail: "
          ++ ashow (proj,ioAssoc,toList <$> toList uniqCandidates)
        Right uniq -> return
          QueryShape
          { qpSchema = sch,qpUnique = deoverlap uniq,qpSize = querySize }
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
    ioAssoc :: [(ShapeSym e s,ShapeSym e s)]
    ioAssoc = mapMaybe (\case
                          (o,E0 (NAggr AggrFirst (E0 i))) -> Just (i,o)
                          _                               -> Nothing) proj
    aexpProps :: Aggr (Expr (ShapeSym e s))
              -> Either (QueryShapeError e s) ColumnProps
    aexpProps agg = do
      ty <- aggrType litType qshape agg
      cnst <- allM (shapeSymConst qshape) $ toList2 agg
      return ColumnProps { columnPropsCppType = ty,columnPropsConst = cnst }

getRowSize :: [(ShapeSym e s,ColumnProps)]
           -> Either (QueryShapeError e s) Bytes
getRowSize sch =
  maybe (throwAStr $ "schemaSize failed for: " ++ ashow sch) return
  $ schemaSize
  $ fmap ((,()) . columnPropsCppType . snd) sch

mapMaybeNEL :: HasCallStack =>
              (NEL.NonEmpty a -> Maybe (NEL.NonEmpty b))
            -> NEL.NonEmpty (NEL.NonEmpty a)
            -> Either (QueryShapeError e s) (NEL.NonEmpty (NEL.NonEmpty b))
mapMaybeNEL f =
  maybe (throwAStr "oops") return
  -- ALL the subexp should be translatable (not realistic FIXME)
  . NEL.nonEmpty
  . mapMaybe f
  . toList

getQueryShapePrj :: forall e s . (HasCallStack,Hashables2 e s) =>
                  (e -> Maybe CppType)
                -> [(ShapeSym e s,Expr (ShapeSym e s))]
                -> QueryShape e s
                -> Either (QueryShapeError e s) (QueryShape e s)
getQueryShapePrj litType proj qshape = do
  sch <- projQueryShapeInternal' =<< traverse3 go proj
  rowSize <- getRowSize sch
  case mapMaybeNEL (traverse (`lookup` ioAssoc)) (qpUnique qshape) of
    Left _ -> throwAStr
      $ "No unique sets exposed in their entirety: "
      ++ ashow
        (toList <$> toList (qpUnique qshape),ioAssoc,qpSchema qshape,proj)
    Right uniq -> return
      QueryShape
      { qpSchema = sch
       ,qpUnique = uniq
       ,qpSize = putRowSize rowSize $ qpSize qshape
      }
  where
    ioAssoc = mapMaybe (\case
                          (o,E0 i) -> Just (i,o)
                          _        -> Nothing) proj
    go :: ShapeSym e s -> Either (QueryShapeError e s) ColumnProps
    go e = do
      ty <- shapeSymType' litType [qshape] e
      cnst <- shapeSymConst qshape e
      return ColumnProps { columnPropsCppType = ty,columnPropsConst = cnst }

projQueryShapeInternal'
  :: Hashables2 e s
  => [(ShapeSym e s,Expr ColumnProps)]
  -> Either (QueryShapeError e s) [(ShapeSym e s,ColumnProps)]
projQueryShapeInternal' proj = forM proj $ \(sym,expt) -> do
  ty <- exprCppType'' $ (,Nothing) . columnPropsCppType <$> expt
  return
    (sym
    ,ColumnProps
       { columnPropsCppType = ty
        ,columnPropsConst = all columnPropsConst expt
       })

-- | Build the symbol shape throwing an error if any Nothings come up.
getSymShape
  :: forall er e s m .
  (Hashables2 e s,AShowError e s er,MonadError er m)
  => Maybe [e]
  -> Maybe (CppSchema' e)
  -> Maybe TableSize
  -> s
  -> m (QueryShape e s)
getSymShape prims symSchema tableSizeM s = do
  sch :: CppSchema' e
    <- maybe (throwAStr "No schema for table") return symSchema
  schAnnotUniq :: [(CppType,(ShapeSym e s,Bool))] <- traverse2 makeSym sch
  when (null schAnnotUniq) $ throwAStr $ "Table has empty schema: " ++ ashow s
  unless (any (snd . snd) schAnnotUniq)
    $ throwAStr
    $ "No unique columns: "
    ++ ashow (s,first shapeSymQnfOriginal . snd <$> schAnnotUniq)
  tableSize <- maybe
    (throwAStr $ "Can't determine size of: " ++ ashow s)
    return
    tableSizeM
  maybe (throwAStr $ "No unique columns: " ++ ashow s) return
    $ mkQueryShape (symSize tableSize) schAnnotUniq
  where
    symSize tableSize = QuerySize { qsTables = tableSize,qsCertainty = 1 }
    makeSym :: Monad m => e -> m (ShapeSym e s,Bool)
    makeSym e = do
      pks <- maybe (throwAStr "Missing primkeys") return prims
      es <- maybe (throwAStr "Missing primkeys") return $ fmap2 snd symSchema
      let isPrim = e `elem` pks
          (nm,_) = nqnfSymbol es s
      shapeSym :: ShapeSym e s
        <- either (const $ throwAStr "mkSymShapeSymNM failed") return
        $ mkSymShapeSymNM nm e
      return (shapeSym,isPrim)
