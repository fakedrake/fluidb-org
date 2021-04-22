{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-simplifiable-class-constraints -Wno-unused-top-binds #-}

module Data.CnfQuery.SanityCheck (sanityCheckRes) where

import           Control.Monad.Except
import           Data.Bitraversable
import           Data.CnfQuery.BuildUtils
import           Data.CnfQuery.HashBag
import           Data.CnfQuery.Types
import           Data.List.Extra
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Functors
import           Data.Utils.Hashable

sanityCheckColsNCNF :: (AShowError e s err, MonadError err m,
                       HashableCNF f e s,Hashables2 e s, Foldable f) =>
                      NCNFQueryF f e s -> m ()
sanityCheckColsNCNF (cols,cnf) =
  aassert (all (`cnfIsColOf` cnf) cols) "Namemap cols do not correspond."

-- | Check that the symbols in the operator are columns of the cnf in
-- the result and output elements of the assoc.
sanityCheckOp :: forall a e s f err m .
                (AShowError e s err, MonadError err m, IOOp f, HasCallStack,
                 Hashables2 e s) =>
                ((AShow e,AShow s) => String)
              -> (a -> f (CNFName e s, e))
              -> NCNFResultI a e s
              -> m ()
sanityCheckOp msg f NCNFResult{..} =
  forM_ ins (checkSym fst) >> forM_ outs (checkSym snd)
  where
    (ins,outs) = opInOutSyms $ f ncnfResOrig
    checkSym :: (forall x . (x,x) -> x) -> (CNFName e s, e) -> m ()
    checkSym side (cnfName,cnfE) = case cnfName of
      Column col 0 -> do
        whenOut side
          $ aassert (cnfIsColOf col $ snd ncnfResNCNF)
          $  "[" ++ msg ++ "] Not a column: "
          ++ ashow (ashowOp $ f ncnfResOrig,outs)
        case (side <$> ncnfResInOutNames,
              filter ((== (cnfE,col)) . side) ncnfResInOutNames) of
          ([],_) -> return ()
          (a,[]) -> throwAStr $ "Not in output side of assocs: "
                   ++ ashow ((cnfE,col),a)
          _ -> return ()
      Column _ _ -> whenOut side
        $ throwAStr "All columns refer to the same cnf so must have 0 index."
      PrimaryCol{} -> whenOut side
        $ throwAStr
        $ "Since we are refering to output we can't have"
        ++ " primary columns in op since at the very least the op itself "
        ++ " is included in the op."
      _ -> return ()
    whenOut :: ((m (),m ()) -> m ()) -> m () -> m ()
    whenOut side m = side (return (),m)

-- | Assert that columns directly refer to the product
sanityCheckCnf :: forall e s d f m err c .
                 (Hashables2 e s, HasCallStack,
                  AShowError e s err, MonadError err m,
                  Bitraversable c, Foldable f) =>
                 ((AShow e, AShow s) => String) -> CNFQueryDCF d c f e s -> m ()
sanityCheckCnf msg cnf = void $ assertOk $ cnfColumns cnf where
  assertOk = bitraverse
    (mapM_ (uncurry assertInProd) . mapMaybe fromCol . toList2)
    (mapM_ (uncurry assertInProd) . mapMaybe fromCol . toList3 . fst)
  assertInProd :: Int -> Either s (CNFCol e s) -> m ()
  assertInProd i s = unless (isInProd i s)
    $ throwAStr $ "SANITY: (" ++ msg ++ "): "
    ++ ashow (i,s) ++ " not in " ++ ashow prods
  isInProd :: Int -> Either s (CNFCol e s) -> Bool
  isInProd i = \case
    Left s -> i < prodMultiplicitySym s
    Right col -> i < prodMultiplicityCol col
  prods = (>>= toList2) $ bagToList $ cnfProd cnf
  fromCol = \case
    Column c i -> Just (i,Right c)
    PrimaryCol _ s i -> Just (i,Left s)
    _ -> Nothing
  prodMultiplicity f =
    maybe 0 snd . listToMaybe . filter (any2 f . fst) $ bagToAssoc $ cnfProd cnf
  prodMultiplicityCol col = prodMultiplicity $ either (const False) (cnfIsColOf col)
  prodMultiplicitySym = prodMultiplicity . (==) . Left

-- | Check that prod doesn't have the right.
sanityCheckProd :: forall e s err  m a .
                  (Hashables2 e s, MonadError err m, AShowError e s err) =>
                  NCNFResultI a e s -> m ()
sanityCheckProd = mapM_ sanityCheckProd0 . cnfProd . snd . ncnfResNCNF
sanityCheckProd0 :: forall e s err  m .
                  (Hashables2 e s, MonadError err m, AShowError e s err) =>
                  CNFProd e s -> m ()
sanityCheckProd0 = mapM_ go where
  err :: ((AShowV e,AShowV s) => String) -> m ()
  err o = throwAStr $ "Op should have been in cnf: " ++ ashow o
  go :: Query (CNFName e s) a -> m ()
  go = \case
    Q2 o l r -> case o of
      QJoin _ -> err $ ashow o
      QProd   -> err $ ashow o
      _       -> go l >> go r
    Q1 o q -> case o of
      QProj _    -> err $ ashow o
      QGroup _ _ -> err $ ashow o
      QSel _     -> err $ ashow o
      _          -> go q
    Q0 _ -> return ()

class Functor op => IOOp op where
  ashowOp :: AShowV a => op a -> SExp
  opInOutSyms :: op a -> ([a],[a])
instance IOOp BQOp where
  ashowOp = ashow'
  opInOutSyms = \case
    QLeftAntijoin _ -> ([],[])
    QRightAntijoin _ -> ([],[])
    o -> ([],toList o)
instance IOOp op => IOOp (Compose Maybe op) where
  ashowOp = ashow' . fmap ashowOp . getCompose
  opInOutSyms = maybe ([],[]) opInOutSyms . getCompose
instance IOOp UQOp where
  ashowOp = ashow'
  opInOutSyms = \case
    QProj p -> (toList2 $ snd <$> p, fst <$> p)
    QGroup p es -> (toList2 es ++ toList2 (toList3 . snd <$> p), fst <$> p)
    x -> ([],toList x)

sanityCheckInOut :: (AShowError e s err, MonadError err m, Hashables2 e s) =>
                   [((e, CNFCol e s), (e, CNFCol e s))] -> m ()
sanityCheckInOut assoc =
  aassert (length (nubOn (snd . snd) assoc) == length assoc)
  "Multiplicity in output columns"

sanityCheckHash :: forall m e s err .
                  (Hashables2 e s, AShowError e s err,
                   MonadError err m, HasCallStack) =>
                  NCNFQueryI e s -> m ()
sanityCheckHash (nm,cnf) = do
  mapM_ sanityCheckCNFHash nm
  sanityCheckCNFHash cnf
  where
    sanityCheckName = \case
      Column col _ -> sanityCheckCNFHash col
      _ -> return ()
    sanityCheckCNFHash :: (Foldable f, Hashable (Either (CNFProj f e s) (CNFAggr f e s)))  =>
                         CNFQueryF f e s -> m ()
    sanityCheckCNFHash cnf0 = do
      aassert (cnfHash cnf0 == (hash $ cnfColumns cnf0,
                               hash $ cnfSel cnf0,
                               hash $ cnfProd cnf0))
        "Hashing error."
      mapM_ sanityCheckCNFHash $ toList4 $ cnfProd cnf0
      mapM_ sanityCheckName $ case cnfColumns cnf0 of
        Left x      ->  toList2 x
        Right (p,g) -> toList3 p ++ toList2 g

#if 0
sanityCheckRes :: (AShowError e s err, MonadError err m, HasCallStack,
                  IOOp f, Hashables2 e s) =>
                 ((AShow e,AShow s) => String)
               -> (a -> f (CNFName e s, e))
               -> NCNFResultI a e s
               -> m (NCNFResultI a e s)
sanityCheckRes msg f res = do
  sanityCheckOp msg f res
  sanityCheckProd res
  sanityCheckHash $ ncnfResNCNF res
  sanityCheckColsNCNF $ ncnfResNCNF res
  sanityCheckInOut $ ncnfResInOutNames res
  forM_ (fst $ ncnfResNCNF res) $ sanityCheckCnf msg
  forM_ (ncnfResNCNF res) $ sanityCheckCnf msg
  forM_ (ncnfResInOutNames res) $ \((_,c),(_,c')) ->
    sanityCheckCnf (msg ++ "[inp symbol]") c
    >> sanityCheckCnf (msg ++ "[out symbol]") c'
  return res
#else
sanityCheckRes :: (AShowError e s err, MonadError err m, HasCallStack,
                  IOOp f, Hashables2 e s) =>
                 ((AShow e,AShow s) => String)
               -> (a -> f (CNFName e s, e))
               -> NCNFResultI a e s
               -> m (NCNFResultI a e s)
sanityCheckRes _ _ = return
#endif
