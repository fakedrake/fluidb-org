{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Data.Query.Optimizations.RemapUnique
  (remapUnique
  ,disambEq
  ,PrelimShape(..)) where

import           Control.Monad.Except
import qualified Data.HashSet         as HS
import           Data.List
import qualified Data.List.NonEmpty   as NEL
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           GHC.Base             (Applicative (liftA2))


data PrelimShape e s =
  PrelimShape { psUniq :: [HS.HashSet e],psExtent :: [e],psQuery :: Query e s }

-- | When there is an equality between unique keys we don't need to
-- keep both of them:
--
-- > disambEq (foldr1Unsafe And $ [P0 (R2 REq (R0 (E0 l)) (R0 (E0 r))) | (r,l) <- [(1,2),(2,3)]]) [1,2,3,4]
-- [[1,4],[2,4],[3,4]]
disambEq
  :: forall e . Hashables1 e => Prop (Rel (Expr e)) -> HS.HashSet e -> [HS.HashSet e]
disambEq p uniqs0 = HS.fromList <$> sequence uniqsPrim
  where
    -- eqGrps are groups of symbols that are equal to each
    -- other. These groups are obviously non-empty and disjoint
    eqGrps = go eqs
      where
        overlaps x y = any (`elem` y) x
        go [] = []
        go (e:es) = nub (join $ e:grp):go rst where
          (grp,rst) = span (overlaps e) es
        eqs = toList (propCnfAnd p) >>= \case
          P0 (R2 REq (R0 (E0 x)) (R0 (E0 y))) -> [[x,y] | x /= y]
          _                                   -> []
    -- instead of each element of the uniqs group we have a list of
    -- equivalent symbols
    uniqsPrim = nub [maybe [u] (\case {[] -> undefined;xs -> xs})
                     $ find (elem u) eqGrps
                    | u <- toList uniqs0]

remapUnique
  :: (MonadPlus m,Hashables1 e)
  => Query e (PrelimShape e s)
  -> m (PrelimShape e s)
remapUnique = go where
  go = \case
    Q0 a      -> return a
    Q1 o q    -> remapUniqueU1 o =<< go q
    Q2 o q q' -> remapUniqueB1 o <$> go q <*> go q'

remapUniqueB1
  :: Hashables1 e
  => BQOp e
  -> PrelimShape e s
  -> PrelimShape e s
  -> PrelimShape e s
remapUniqueB1 o l r =
  PrelimShape
  { psQuery = Q2 o (psQuery l) (psQuery r),psUniq = uniq,psExtent = extent }
  where
    (extent,uniq) = case o of
      -- Note: n case of a join that is a foreign key join, we have the
      -- opportunity to drop one completely. This is implemented in the
      -- case of QueryPlan `joinPlan`. TODO: Use QueryPlans here too.
      QJoin p ->
        (combExtent,(<>) <$> psUniq l <*> psUniq r >>= disambEq p)
      QProd -> (combExtent,(<>) <$> psUniq l <*> psUniq r)
      QUnion -> (psExtent l,psUniq l)
      QDistinct -> (psExtent l,psUniq l) -- We will be grouping by these so they will be uniq
      QProjQuery -> error "We can't infer unique set for QProjQuery"
      QLeftAntijoin _ -> (psExtent l,psUniq l)
      QRightAntijoin _ -> (psExtent l,psUniq r)
    combExtent = psExtent l ++ psExtent r

-- Count remapd symbols.
remapUniqueU1
  :: forall e s m .
  (MonadPlus m,Hashables1 e)
  => UQOp e
  -> PrelimShape e s
  -> m (PrelimShape e s)
remapUniqueU1 o ps@PrelimShape {..} = case o of
  -- A projection needs to have a unique subtuple in both the remapd
  -- set and the complement. First select a unique tuple. Check what
  -- parts of it are directly remapd and what parts of it are in the
  -- complement.
  QProj QProjNoInv p -> do
    (uniq,prj) <- mkProj p psExtent psUniq
    return
      PrelimShape
      { psQuery = Q1 (toQProj prj) psQuery
       ,psUniq = uniq
       ,psExtent = fst <$> qpneProj prj
      }
  QProj _ _ -> error "We got an already complemented projection."
  QGroup p g -> do
    (uniq,prj,grp) <- mkGrp (p,g) psExtent psUniq
    return
      PrelimShape
      { psQuery = Q1 (QGroup prj grp) psQuery
       ,psUniq = uniq
       ,psExtent = fst <$> prj
      }
  -- Note: see join for explanation o disambEq (there may be a join
  -- hidden in the selection)
  QSel _ -> pure ps'
  QSort _ -> pure ps'
  QDrop _ -> pure ps'
  QLimit _ -> pure ps'
  where
    ps' = ps { psQuery = Q1 o psQuery }

-- | When the grouping is global all columns are unique.
--
-- Otherwise first check if unique keys are already exposed and check
-- that the grouping predicates are simple columns (in which case they
-- are uniques).
mkGrp
  :: forall e m .
  (Hashables1 e,MonadPlus m)
  => ([(e,Expr (Aggr (Expr e)))],[Expr e])
  -> [e]
  -> [HS.HashSet e]
  -> m ([HS.HashSet e],[(e,Expr (Aggr (Expr e)))],[Expr e])
mkGrp (prj,grp) inputExtent inpUniqs = case NEL.nonEmpty grp of
  Nothing    -> return ([HS.singleton e | (e,_) <- prj],prj,[])
  Just grpNe -> go grpNe
  where
    go :: NEL.NonEmpty (Expr e)
       -> m ([HS.HashSet e],[(e,Expr (Aggr (Expr e)))],[Expr e])
    go grpNe = case remapdUniqs of
      _:_ -> return (remapdUniqs,prj,grp)
      []  -> foldr loop mzero allUniqs
      where
        allUniqs = inpUniqs <> toList (asSyms grpNe)
        -- Symbols that are (e,E0 e)
        remaps = mapMaybe extractAggrRemap prj
        -- What part of the input extent is remaped?
        (inpExtentRemap,_inpExtentCompl) =
          partition (`elem` fmap snd remaps) inputExtent
        remapdUniqs =
          filter (`HS.isSubsetOf` HS.fromList inpExtentRemap) allUniqs
        loop :: HS.HashSet e
             -> m ([HS.HashSet e],[(e,Expr (Aggr (Expr e)))],[Expr e])
             -> m ([HS.HashSet e],[(e,Expr (Aggr (Expr e)))],[Expr e])
        loop uniq nxt = (`mplus` nxt) $ do
          let (_remapdUniq0,nonRemapdUniq0) =
                partition (`elem` fmap snd remaps) $ toList uniq
          let extraGrp =
                [(u,E0 $ NAggr AggrFirst $ E0 u) | u <- nonRemapdUniq0]
          return ([uniq],extraGrp ++ prj,grp)


-- | If all the expressions are simple symbols make a set out of
-- them.
asSyms :: Hashables1 e => NEL.NonEmpty (Expr e) -> Maybe (HS.HashSet e)
asSyms = foldr (liftA2 HS.insert) (Just mempty) . fmap asSym
  where
    asSym = \case
      E0 e -> Just e
      _    -> Nothing

-- First check that there are remapped unique subtuples. If they are
-- construct the naive complement by getting the non remapd columns
-- of the input extent and adding each unique subtuple non
-- deterministically. The only valid unique subtuple is the one
-- registered as an overlap between the complement and the remapd.
--
-- If there are no remapd unique subtuples, for each unique subtuple
-- of the input, remap the non-remapd part of it and repeat the
-- process.
mkProj
  :: (Hashables1 e,MonadPlus m)
  => [(e,Expr e)]
  -> [e]
  -> [HS.HashSet e]
  -> m ([HS.HashSet e],QProjNonEmpty e)
mkProj prj inputExtent allUniqs = case remapdUniqs of
  [] -> go allUniqs
  uniqs -> do
    uniq <- msum $ return <$> uniqs
    let uniqList = toList uniq
    return
      ([uniq]
      ,QProjNonEmpty
       { qpneCompl = inpExtentCompl <> uniqList
        ,qpneOverlap = [(e,e) | e <- uniqList]
        ,qpneProj = prj
       })
  where
    -- Symbols that are (e,E0 e)
    remaps = mapMaybe extractProjRemap prj
    -- What part of the input extent is remaped?
    (inpExtentRemap,inpExtentCompl) =
      partition (`elem` fmap snd remaps) inputExtent
    remapdUniqs = filter (`HS.isSubsetOf` HS.fromList inpExtentRemap) allUniqs
    go [] = mzero
    go (uniq:uniqs) = (`mplus` go uniqs) $ fmap ([uniq],) $ do
      let uniqList = HS.toList uniq
      let (remapdUniq0,nonRempedUniq0) =
            partition (`elem` fmap snd remaps) uniqList
      let complement = inpExtentCompl ++ remapdUniq0
      let prjExtra = [(u,E0 u) | u <- nonRempedUniq0]
      let qpneProj = prjExtra ++ prj
      let qpneCompl = complement
      let qpneOverlap = [(u,u) | u <- uniqList]
      return QProjNonEmpty { .. }

extractAggrRemap :: (e, Expr (Aggr (Expr e0))) -> Maybe (e0, e)
extractAggrRemap = \case
  (e,E0 (NAggr AggrFirst (E0 ei))) -> Just (ei,e)
  _                                -> Nothing
extractProjRemap :: (e, Expr e0) -> Maybe (e,e0)
extractProjRemap = \case
  (e,E0 e') -> Just (e,e')
  _         -> Nothing
