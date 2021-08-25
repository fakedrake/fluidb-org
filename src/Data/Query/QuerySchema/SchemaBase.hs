{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
module Data.Query.QuerySchema.SchemaBase
  (shapeSymOrig
  ,isUniqueSel
  ,mkQueryShape
  ,mkShapeSym
  ,setShapeSymOrig
  ,refersToShape
  ,querySchema
  ,primKeys
  ,joinShapes
  ,shapeAllSyms
  ,shapeProject
  ,shapeSymIsSym
  ,shapeSymTypeSym'
  ,mkSymShapeSymNM
  ,complementProj
  ,mkLitShapeSym
  ,schemaQP
  ,lookupQP
  ,shapeSymEqs
  ,translateShape
  ,translateShape'
  ,translateShapeMap'') where

import           Control.Monad.Reader
import           Data.Bitraversable
import           Data.CppAst
import           Data.Functor.Identity
import qualified Data.HashMap.Strict          as HM
import           Data.List
import qualified Data.List.NonEmpty           as NEL
import           Data.Maybe
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.Query.QuerySize
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable

mkLitShapeSym :: e -> ShapeSym e s
mkLitShapeSym e = mkShapeSym (NonSymbolName e) e

mkSymShapeSymNM :: Hashables2 e s =>
                 NameMap e s -> e -> Either (QNFError e s) (ShapeSym e s)
mkSymShapeSymNM nm e = (`mkShapeSym` e) . (`Column` 0) <$> getName nm e

shapeSymIsSym :: ShapeSym e s -> Bool
shapeSymIsSym ps = case  shapeSymQnfName ps of
  NonSymbolName _ -> False
  _               -> True

shapeSymOrig :: ShapeSym e s -> e
shapeSymOrig = shapeSymQnfOriginal

mkShapeSym :: QNFName e s -> e -> ShapeSym e s
mkShapeSym qnfn e = ShapeSym{shapeSymQnfOriginal=e,shapeSymQnfName=qnfn}

setShapeSymOrig :: e -> ShapeSym e s -> ShapeSym e s
setShapeSymOrig e ps = ps{shapeSymQnfOriginal=e}

-- | Nothing means it's a literal.
refersToShape :: Hashables2 e s => ShapeSym e s -> QueryShape e s -> Maybe Bool
refersToShape ps shape = case shapeSymQnfName ps of
  NonSymbolName _ -> Nothing
  _               -> Just $ isJust $ lookupQP ps shape

-- | NOT AN ISOMORPHISM
schemaQP :: QueryShape e s -> [(ShapeSym e s,ColumnProps)]
schemaQP = qpSchema

primKeys :: QueryShape e s -> NEL.NonEmpty (NEL.NonEmpty (ShapeSym e s))
primKeys = qpUnique

shapeAllSyms :: QueryShape e s -> [ShapeSym e s]
shapeAllSyms = fmap fst . schemaQP

querySchema :: QueryShape e s -> CppSchema' (ShapeSym e s)
querySchema
  shape = [(columnPropsCppType,ps) | (ps,ColumnProps {..}) <- schemaQP shape]

-- XXX: increment indices

-- | Filter the unique ones. None means there were no uniques (or the
-- shape was empty.
mkQueryShape :: TableSize -> [(CppType,(e,Bool))] -> Maybe (QueryShape' e)
mkQueryShape tblSize sch =
  fromSchemaQP
    [(sym
     ,ColumnProps { columnPropsConst = False,columnPropsCppType = ty }
     ,uniq) | (ty,(sym,uniq)) <- sch]
  where
    fromSchemaQP :: [(e,ColumnProps,Bool)] -> Maybe (QueryShape' e)
    fromSchemaQP
      sch = NEL.nonEmpty [s | (s,_,isU) <- sch,isU] <&> \uniq -> QueryShape
      { qpSchema = [(s,prop) | (s,prop,_isU) <- sch]
       ,qpUnique = return uniq
       ,qpSize = QuerySize { qsTables = [tblSize],qsCertainty = 1 }
      }


shapeSymTypeSym' :: Hashables2 e s =>
                  [QueryShape e s]
                -> ShapeSym e s
                -> Maybe (Either e CppType)
shapeSymTypeSym' shapes ps = case shapeSymQnfName ps of
  NonSymbolName e -> Just $ Left e
  _ -> Right . columnPropsCppType
    <$> listToMaybe (catMaybes $ lookupQP ps <$> shapes)
















































hardLookupQP :: Hashables2 e s =>
               ShapeSym e s -> QueryShape e s -> Maybe ColumnProps
hardLookupQP ps' = lookup ps' . schemaQP
lookupQP :: Hashables2 e s =>
           ShapeSym e s -> QueryShape e s -> Maybe ColumnProps
lookupQP = hardLookupQP
complementProj :: forall e s . Hashables2 e s =>
                 QueryShape e s
               -> [(ShapeSym e s, Expr (ShapeSym e s))]
               -> [(ShapeSym e s, Expr (ShapeSym e s))]
complementProj shape prj = invPrj `union`
  [(e, E0 e) | e <- qsyms, e `notElem` map fst invPrj]
  where
    qsyms :: [ShapeSym e s]
    qsyms = shapeAllSyms shape
    invPrj :: [(ShapeSym e s, Expr (ShapeSym e s))]
    invPrj = mapMaybe (invExpr . snd) prj

invExpr :: Expr (ShapeSym e s)
        -> Maybe (ShapeSym e s, Expr (ShapeSym e s))
invExpr expr = runIdentity $ exprInverse (return . shapeSymIsSym) expr <&> \case
  EInv (x,toX) -> Just (x,toX x)
  _            -> Nothing


assocToMap :: Hashables2 e a => [(e,a)] -> HM.HashMap e [a]
assocToMap assoc = foldl' (\m (s,d) -> HM.alter (Just . maybe [d] (d:)) s m) mempty
  $ reverse assoc

-- | Given an association of in/out versions of the same symbol
-- translate a shape expressed in terms of input symbols as in terms of
-- output symbols. Note that shapes should not have literals but we
-- pass them through.
--
-- Always hard lookups here.
translateShape' :: forall e s . (HasCallStack, Hashables2 e s) =>
                 [(ShapeSym e s, ShapeSym e s)]
               -> QueryShape e s
               -> Either (AShowStr e s) (QueryShape e s)
translateShape' = translateShapeMap'' listToMaybe

translateShapeMap'' :: forall e s . (HasCallStack, Hashables2 e s) =>
                     (forall a . [a] -> Maybe a)
                   -> [(ShapeSym e s, ShapeSym e s)]
                   -> QueryShape e s
                   -> Either (AShowStr e s) (QueryShape e s)
translateShapeMap'' f assoc p =
  traverse (bitraverse safeLookup pure) (qpSchema p) >>= \sch -> do
    qpu <- traverse2 safeLookup $ qpUnique p
    return QueryShape { qpSchema = sch,qpUnique = qpu,qpSize = _ }
  where
    symsMap = assocToMap assoc
    safeLookup :: ShapeSym e s -> Either (AShowStr e s) (ShapeSym e s)
    safeLookup e = case shapeSymQnfName e of
      PrimaryCol {} -> undefined
      _             -> safeLookup0
      where
        safeLookup0 :: Either (AShowStr e s) (ShapeSym e s)
        safeLookup0 = lookup_e `alt` asLiteral
          where
            asLiteral :: Either (AShowStr e s) (ShapeSym e s)
            asLiteral = case shapeSymQnfName e of
              NonSymbolName _ -> Right e
              _               -> throwAStr $ "Expected literal:" ++ ashow e
            lookup_e :: Either (AShowStr e s) (ShapeSym e s)
            lookup_e = case HM.lookup e symsMap >>= f of
              Nothing -> throwAStr
                $ "Lookup failed:"
                ++ ashow (e,shapeAllSyms p,HM.toList symsMap)
              Just x -> Right x
            -- | Prefer the left hand side error
            alt l@(Right _) _ = l
            alt _ r@(Right _) = r
            alt l _           = l

translateShape :: (HasCallStack, Hashables2 e s) =>
                [(ShapeSym e s, ShapeSym e s)]
              -> QueryShape e s
              -> Maybe (QueryShape e s)
translateShape = either (const Nothing) Just ... translateShape'
isUniqueSel :: Hashables2 e s =>
              QueryShape e s
            -> [(Expr (ShapeSym e s),Expr (ShapeSym e s))]
            -> Bool
isUniqueSel qp eqs = (`any` qpUnique qp) $ \ukeys ->
  (`all` ukeys)
  $ \uk -> (`any` (eqs ++ fmap flip eqs))
  $ \(el,er) ->
      E0 uk == el && not ((`any` er) $ \e -> e `elem` allKeys)
  where
    flip (a,b) = (b,a)
    allKeys = shapeAllSyms qp

shapeProject :: forall e s . Hashables2 e s =>
              ([QueryShape e s] ->
               Expr (Either CppType (ShapeSym e s)) ->
               Maybe ColumnProps)
            -> [(ShapeSym e s, Expr (Either CppType (ShapeSym e s)))]
            -> QueryShape e s
            -> Maybe (QueryShape e s)
shapeProject exprColumnProps prj shape = do
  symProps <- forM prj $ \(sym,expr) -> (sym,) <$> exprColumnProps [shape] expr
  uniq <- NEL.nonEmpty $ mapMaybe translUniq $ toList $ qpUnique shape
  return $ QueryShape { qpSchema = symProps,qpUnique = uniq,qpSize = _ }
  where
    keyAssoc =
      mapMaybe (\case
                  (e,E0 (Right ev)) -> Just (ev,e)
                  _                 -> Nothing) prj
    translUniq ukeys = mapM (`lookup` keyAssoc) ukeys

-- | Given an assoc and a list all the possible
-- λ> substitutions [('a','A')] ['a','b']
-- ('a' :| "b") :| ['A' :| "b"
-- λ> substitutions [('x','X')] ['a','b']
-- ('a' :| "b") :| []
substitutions :: forall a . Eq a =>
                [(a,a)] -> NEL.NonEmpty a -> NEL.NonEmpty (NEL.NonEmpty a)
substitutions eqs uniqs0 =
  foldl (\uniqs' eqpair -> disamb eqpair =<< uniqs') (return uniqs0) eqs
  where
    disamb (x,y) uniqs' =
      uniqs' NEL.:| toList (replace x y uniqs') ++ toList (replace y x uniqs')
    replace :: a -> a -> NEL.NonEmpty a -> Maybe (NEL.NonEmpty a)
    replace f t es = if f `elem` es
      then Just $ (\e -> if e == f then t else e) <$> es
      else Nothing

-- | Concatenate shapes given an equality between them. The equality
-- makes for different unique sets.
joinShapes
  :: forall e .
  Eq e
  => [(e,e)]
  -> QueryShape' e
  -> QueryShape' e
  -> Maybe (QueryShape' e)
joinShapes eqs qpL qpR =
  uniqDropConst QueryShape { qpSchema = sch,qpUnique = uniq,qpSize = _ }
  where
    sch = qpSchema qpL ++ qpSchema qpR
    uniq :: NEL.NonEmpty (NEL.NonEmpty e)
    uniq = do
      usetL :: NEL.NonEmpty e <- qpUnique qpL
      usetR :: NEL.NonEmpty e <- qpUnique qpR
      -- Check if the equalities are lookups
      let luOnL = all (`elem` fmap fst normEqs) usetL
      let luOnR = all (`elem` fmap snd normEqs) usetR
      case (luOnR,luOnL) of
        (False,True) -> substitutions normEqs usetL
        (True,False) -> substitutions (swap <$> normEqs) usetR
        (_,_) -> NEL.nub
          $ substitutions normEqs usetL
          <> substitutions (swap <$> normEqs) usetR
    normEqs = mapMaybe (uncurry go) eqs
      where
        go l r =
          if
            | linl && not linr && rinr && not rinl -> Just (l,r)
            | not linl && linr && not rinr && rinl -> Just (r,l)
            | (linl && linr) || (rinl && rinr)
              -> error "Exposed symbols should be disjoint in products"
            | otherwise -> Nothing
          where
            linl = l `inAndNonConst` qpSchema qpL
            linr = l `inAndNonConst` qpSchema qpR
            rinl = r `inAndNonConst` qpSchema qpL
            rinr = r `inAndNonConst` qpSchema qpR
            inAndNonConst e s = case columnPropsConst <$> lookup e s of
              Just False -> True
              _          -> False

-- | Drop constant columns
uniqDropConst :: Eq e => QueryShape' e -> Maybe (QueryShape' e)
uniqDropConst p =
  fmap (\uniq -> p { qpUnique = uniq })
  $ NEL.nonEmpty
  -- Note: if we have ONLY consts in a unique set something is wrong
  -- or we called this function at the wrong place (not over a
  -- product-like)
  $ mapMaybe (NEL.nonEmpty . filter isNotConst . toList)
  $ toList
  $ qpUnique p
  where
    isNotConst e = case fmap columnPropsConst $ lookup e $ qpSchema p of
      Just False -> True
      _          -> False

shapeSymEqs :: Hashables2 e s =>
             Prop (Rel (Expr (ShapeSym e s)))
           -> [(ShapeSym e s,ShapeSym e s)]
shapeSymEqs p = mapMaybe asEq $ toList $ propQnfAnd p where
  asEq :: Prop (Rel (Expr (ShapeSym e s))) -> Maybe (ShapeSym e s,ShapeSym e s)
  asEq = \case
    P0 (R2 REq (R0 (E0 l)) (R0 (E0 r))) ->
      if shapeSymIsSym l && shapeSymIsSym r
      then Just (l,r)
      else Nothing
    _ -> Nothing
