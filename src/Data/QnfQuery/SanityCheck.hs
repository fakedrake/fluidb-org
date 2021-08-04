{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-simplifiable-class-constraints -Wno-unused-top-binds -Wno-deprecations #-}

module Data.QnfQuery.SanityCheck (sanityCheckRes) where

import           Control.Monad.Except
import           Data.Bitraversable
import           Data.List.Extra
import           Data.Maybe
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.HashBag
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Functors
import           Data.Utils.Hashable

sanityCheckColsNQNF :: (AShowError e s err, MonadError err m,
                       HashableQNF f e s,Hashables2 e s, Foldable f) =>
                      NQNFQueryF f e s -> m ()
sanityCheckColsNQNF (cols,qnf) =
  aassert (all (`qnfIsColOf` qnf) cols) "Namemap cols do not correspond."

-- | Check that the symbols in the operator are columns of the qnf in
-- the result and output elements of the assoc.
sanityCheckOp :: forall a e s f err m .
                (AShowError e s err, MonadError err m, IOOp f, HasCallStack,
                 Hashables2 e s) =>
                ((AShow e,AShow s) => String)
              -> (a -> f (QNFName e s, e))
              -> NQNFResultI a e s
              -> m ()
sanityCheckOp msg f NQNFResult{..} =
  forM_ ins (checkSym fst) >> forM_ outs (checkSym snd)
  where
    (ins,outs) = opInOutSyms $ f nqnfResOrig
    checkSym :: (forall x . (x,x) -> x) -> (QNFName e s, e) -> m ()
    checkSym side (qnfName,qnfE) = case qnfName of
      Column col 0 -> do
        whenOut side
          $ aassert (qnfIsColOf col $ snd nqnfResNQNF)
          $  "[" ++ msg ++ "] Not a column: "
          ++ ashow (ashowOp $ f nqnfResOrig,outs)
        case (side <$> nqnfResInOutNames,
              filter ((== (qnfE,col)) . side) nqnfResInOutNames) of
          ([],_) -> return ()
          (a,[]) -> throwAStr $ "Not in output side of assocs: "
                   ++ ashow ((qnfE,col),a)
          _ -> return ()
      Column _ _ -> whenOut side
        $ throwAStr "All columns refer to the same qnf so must have 0 index."
      PrimaryCol{} -> whenOut side
        $ throwAStr
        $ "Since we are refering to output we can't have"
        ++ " primary columns in op since at the very least the op itself "
        ++ " is included in the op."
      _ -> return ()
    whenOut :: ((m (),m ()) -> m ()) -> m () -> m ()
    whenOut side m = side (return (),m)

-- | Assert that columns directly refer to the product
sanityCheckQnf :: forall e s d f m err c .
                 (Hashables2 e s, HasCallStack,
                  AShowError e s err, MonadError err m,
                  Bitraversable c, Foldable f) =>
                 ((AShow e, AShow s) => String) -> QNFQueryDCF d c f e s -> m ()
sanityCheckQnf msg qnf = void $ assertOk $ qnfColumns qnf where
  assertOk = bitraverse
    (mapM_ (uncurry assertInProd) . mapMaybe fromCol . toList2)
    (mapM_ (uncurry assertInProd) . mapMaybe fromCol . toList2 . fst)
  assertInProd :: Int -> Either s (QNFCol e s) -> m ()
  assertInProd i s = unless (isInProd i s)
    $ throwAStr $ "SANITY: (" ++ msg ++ "): "
    ++ ashow (i,s) ++ " not in " ++ ashow prods
  isInProd :: Int -> Either s (QNFCol e s) -> Bool
  isInProd i = \case
    Left s    -> i < prodMultiplicitySym s
    Right col -> i < prodMultiplicityCol col
  prods = (>>= toList2) $ bagToList $ qnfProd qnf
  fromCol = \case
    Column c i       -> Just (i,Right c)
    PrimaryCol _ s i -> Just (i,Left s)
    _                -> Nothing
  prodMultiplicity f =
    maybe 0 snd . listToMaybe . filter (any2 f . fst) $ bagToAssoc $ qnfProd qnf
  prodMultiplicityCol col = prodMultiplicity $ either (const False) (qnfIsColOf col)
  prodMultiplicitySym = prodMultiplicity . (==) . Left

-- | Check that prod doesn't have the right.
sanityCheckProd :: forall e s err  m a .
                  (Hashables2 e s, MonadError err m, AShowError e s err) =>
                  NQNFResultI a e s -> m ()
sanityCheckProd = mapM_ sanityCheckProd0 . qnfProd . snd . nqnfResNQNF
sanityCheckProd0 :: forall e s err  m .
                  (Hashables2 e s, MonadError err m, AShowError e s err) =>
                  QNFProd e s -> m ()
sanityCheckProd0 = mapM_ go where
  err :: ((AShowV e,AShowV s) => String) -> m ()
  err o = throwAStr $ "Op should have been in qnf: " ++ ashow o
  go :: Query (QNFName e s) a -> m ()
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
    QLeftAntijoin _  -> ([],[])
    QRightAntijoin _ -> ([],[])
    o                -> ([],toList o)
instance IOOp op => IOOp (Compose Maybe op) where
  ashowOp = ashow' . fmap ashowOp . getCompose
  opInOutSyms = maybe ([],[]) opInOutSyms . getCompose
instance IOOp UQOp where
  ashowOp = ashow'
  opInOutSyms = \case
    QProj p     -> (toList2 $ snd <$> p, fst <$> p)
    QGroup p es -> (toList2 es ++ toList2 (toList3 . snd <$> p), fst <$> p)
    x           -> ([],toList x)

sanityCheckInOut :: (AShowError e s err, MonadError err m, Hashables2 e s) =>
                   [((e, QNFCol e s), (e, QNFCol e s))] -> m ()
sanityCheckInOut assoc =
  aassert (length (nubOn (snd . snd) assoc) == length assoc)
  "Multiplicity in output columns"

sanityCheckHash :: forall m e s err .
                  (Hashables2 e s, AShowError e s err,
                   MonadError err m, HasCallStack) =>
                  NQNFQueryI e s -> m ()
sanityCheckHash (nm,qnf) = do
  mapM_ sanityCheckQNFHash nm
  sanityCheckQNFHash qnf
  where
    sanityCheckName = \case
      Column col _ -> sanityCheckQNFHash col
      _            -> return ()
    sanityCheckQNFHash
      :: (Foldable f,Hashable (Either (QNFProj f e s) (QNFAggr f e s)))
      => QNFQueryF f e s
      -> m ()
    sanityCheckQNFHash qnf0 = do
      aassert
        (qnfHash qnf0
         == (hash $ qnfColumns qnf0,hash $ qnfSel qnf0,hash $ qnfProd qnf0))
        "Hashing error."
      mapM_ sanityCheckQNFHash $ toList4 $ qnfProd qnf0
      mapM_ sanityCheckName $ case qnfColumns qnf0 of
        Left x      -> toList2 x
        Right (p,g) -> toList2 p ++ toList2 g

#if 0
sanityCheckRes :: (AShowError e s err, MonadError err m, HasCallStack,
                  IOOp f, Hashables2 e s) =>
                 ((AShow e,AShow s) => String)
               -> (a -> f (QNFName e s, e))
               -> NQNFResultI a e s
               -> m (NQNFResultI a e s)
sanityCheckRes msg f res = do
  sanityCheckOp msg f res
  sanityCheckProd res
  sanityCheckHash $ nqnfResNQNF res
  sanityCheckColsNQNF $ nqnfResNQNF res
  sanityCheckInOut $ nqnfResInOutNames res
  forM_ (fst $ nqnfResNQNF res) $ sanityCheckQnf msg
  forM_ (nqnfResNQNF res) $ sanityCheckQnf msg
  forM_ (nqnfResInOutNames res) $ \((_,c),(_,c')) ->
    sanityCheckQnf (msg ++ "[inp symbol]") c
    >> sanityCheckQnf (msg ++ "[out symbol]") c'
  return res
#else
sanityCheckRes :: (AShowError e s err, MonadError err m, HasCallStack,
                  IOOp f, Hashables2 e s) =>
                 ((AShow e,AShow s) => String)
               -> (a -> f (QNFName e s, e))
               -> NQNFResultI a e s
               -> m (NQNFResultI a e s)
sanityCheckRes _ _ = return
#endif
