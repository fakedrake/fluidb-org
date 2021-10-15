{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing #-}

module Data.Codegen.Build.Constructors
  ( Constr
  , ConstructorArg(..)
  , clusterCall
  , getQueriesFromClust
  , sortCall
  , projCall
  , tmplClass
  , constrArgs
  , opStatements
  , opConstructor
  , selCall
  , prodCall
  , unionCall
  , joinCall
  , dropCall
  , limitCall
  , groupCall
  , constrBlock
  , constrArgsRevUnary
  , constrArgsRevRight
  , constrArgsRevLeft
  , constrArgsRevLeftStr
  , constrArgsRevRightStr
  , unJoinCall
  ) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Propagators
import           Data.Cluster.Types
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build.Classes
import           Data.Codegen.Build.EquiJoinUtils
import           Data.Codegen.Build.Expression
import           Data.Codegen.Build.IoFiles
import           Data.Codegen.Build.Monads
import           Data.Codegen.Schema
import qualified Data.CppAst                      as CC
import           Data.Either
import           Data.Foldable
import           Data.List
import           Data.Maybe
import           Data.Monoid
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.FileSet
import qualified Data.Set                         as DS
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Tup
import           Prelude                          hiding (exp)

type Constr = (CC.FunctionSymbol CC.CodeSymbol, [CC.TmplInstArg CC.CodeSymbol])

tmplClass :: CC.Class a -> CC.TmplInstArg a
tmplClass = CC.TypeArg . CC.ClassType mempty [] . CC.className

-- mkProjection<PrimFn, SecFn>
projCall
  :: forall e s t n m c .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> [(ShapeSym e s,Expr (ShapeSym e s))]
  -> m Constr
projCall UnFwd _ pr = do
  Identity shape <- getQueries
  coPr <- withPrimKeys $ complementProj shape pr
  primFn <- projToFn $ fmap3 Right pr
  coFn <- projToFn $ fmap3 Right coPr
  return ("mkProjection", tmplClass <$> [primFn, coFn])
projCall UnRev outSh pr = do
  combFn <- combinerClass outSh
  return ("mkZip",[tmplClass combFn])


-- mkSort<ToKeyFn>
sortCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => OutShape e s
  -> [Expr (ShapeSym e s)]
  -> m Constr
sortCall OutShape {oaIoAssoc = outAssoc} sr = do
  -- XXX: here we call any schema at some point
  keySch :: CppSchema <- forM (zip [0 :: Int ..] sr) $ \(i,exp) -> do
    ty <- exprCppTypeErr =<< traverse (translateSym $ swap <$> outAssoc) exp
    let sym = fromString $ "sortElem" ++ show i
    return (ty,sym)
  -- Key record class
  keyRec <- tellRecordCls keySch
  let keyRecSym = CC.classNameRef keyRec
  -- Expressions comprising the key
  exprs <- runListT $ do
    exp <- mkListT $ return sr
    lift
      $ exprExpression . fmap Right
      =<< traverse (translateSym $ fmap swap outAssoc) exp
  -- Class function to make key
  toKeyFn <- tellClassM
    $ mkCallClass (CC.ClassType mempty [] keyRecSym)
    $ CC.FunctionAp (CC.SimpleFunctionSymbol keyRecSym) [] exprs
  return ("mkSort",tmplClass <$> [toKeyFn])

-- mkAggregation<FoldFn :: From -> To, StartNew> There is no reverse
-- trigger for groupCall
groupCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => [(ShapeSym e s,Expr (Aggr (Expr (ShapeSym e s))))]
  -> [Expr (ShapeSym e s)]
  -> m Constr
groupCall pr g = do
  codomainCls <- tellRecordCls =<< cppSchema =<< aggrSchema @Identity @e @s pr
  aggrExpressionEither <- aggrProjExpression pr
  foldCls <- tellClassM $ do
    noAggrDecl <- mkCallClass
      -- Codomain
      (CC.ClassType mempty [] $ CC.classNameRef codomainCls)
      -- Expression (domain inferred)
      $ fmap
        (either (CC.runSymbol . CC.declarationNameRef) id)
        aggrExpressionEither
    let aggrVars = lefts $ toList aggrExpressionEither
    return
      noAggrDecl
      { CC.classPrivateMembers =
          fmap CC.MemberVariable aggrVars ++ CC.classPrivateMembers noAggrDecl
      }
  startNewCls <- startNewCallClass g
  return ("mkAggregation",tmplClass <$> [foldCls,startNewCls])

-- mkDrop<Rec, i>
limitCall
  :: forall e s t n m c .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> Int
  -> m Constr
limitCall UnRev io _i = unionCall io
limitCall UnFwd _ i = do
  qrec <- queryRecord . runIdentity =<< getQueries
  return ("mkLimit", [CC.TemplArg $ CC.LiteralIntExpression i, tmplClass qrec])

-- mkLimit<i, Rec>
dropCall
  :: forall e s t c n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> Int
  -> m Constr
dropCall UnFwd _io i = first (const "mkDrop") <$> limitCall UnFwd undefined i
dropCall UnRev io _i = unionCall io

-- EquiJoin<LExtractFn, RExtractFn, Combine>
-- mkJoin<Pred, Comb>
--
-- Note that the C++ library knows that the left and right triangles
-- are the same as the inputs so it reuses those classes. If name
-- erasure for unique names here works properly this should be
-- producing functional code.
joinCall
  :: forall e s t n c m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => BinDir c
  -> OutShape e s
  -> Prop (Rel (Expr (ShapeSym e s)))
  -> m Constr
-- idAssocAndShape is the output shape and id associations for
-- all syms.
joinCall BinRev _ _ = unJoinCall
joinCall BinFwd outShape p = do
  -- Note: A `Rantij` A == A `Lantij` A makes duplicate output columns
  -- so don't expect output symbols to be unique within the list.
  combFn <- combinerClass outShape
  pInSyms <- traverse3 (translateSym $ swap <$> oaIoAssoc outShape) p
  joinPred <- equiJoinPred pInSyms
  fmap3 tmplClass $ case joinPred of
    Just eqClss -> do
      EquiJoinPred {..} <- mkEquiJoinPred eqClss
      return ("mkEquiJoin",[equiJoinLExtract,equiJoinRExtract,combFn])
    Nothing -> do
      propCls <- propCallClass pInSyms
      return ("mkJoin",[propCls,combFn])

extractClass
  :: (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => QueryShape e s
  -> m [CC.TmplInstArg CC.CodeSymbol]
extractClass q =
  snd <$> projCall UnFwd undefined (keysToProj $ snd <$> querySchema q)

-- unJoin<Extract>
unJoinCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => m Constr
unJoinCall = fmap ("mkUnJoin",) $ extractClass . runIdentity =<< getQueries

-- mkUnion<R>
unionCall
  :: forall e s t n m c .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Tup2 e s m)
  => OutShape e s
  -> m Constr
unionCall OutShape {oaIoAssoc = assoc} = do
  Tup2 shapeL shapeR <- getQueries @Tup2 @e @s
  Tup2 projL projR0 <- traverse (symsProj . querySchema) $ Tup2 shapeL shapeR
  aassert
    (isSubsetOf (fst <$> projR0) (fst <$> projL))
    "Union should be extending the second with nulls."
  let projR =
        fmap3 Right projR0
        ++ [(e,E0 $ Left ty) | e@(_,ty)
           <- setDiff (fst <$> projL) (fst <$> projR0)]
  fromL <- evalQueryEnv (Identity shapeL)
    $ projToFn
    $ first fst <$> fmap3 Right projL
  fromR <- evalQueryEnv (Identity shapeR) $ projToFn $ first fst <$> projR
  return ("mkUnion",tmplClass <$> [fromL,fromR])
  where
    setDiff :: Eq a => [a] -> [a] -> [a]
    setDiff xs ys = filter (`notElem` ys) xs
    isSubsetOf :: Eq a => [a] -> [a] -> Bool
    isSubsetOf xs ys = all (`elem` ys) xs
    symsProj :: [(CC.CppType,ShapeSym e s)]
             -> m [((ShapeSym e s,CC.CppType),Expr (ShapeSym e s))]
    symsProj = mapM $ \(ty,s) -> case lookup s assoc of
      Nothing   -> throwAStr $ "Missing assoc for symbol: " ++ ashow (s,assoc)
      Just sOut -> return ((sOut,ty),E0 s)

-- mkProduct<Combine>
prodCall
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Tup2 e s m)
  => OutShape e s
  -> m Constr
prodCall outAssocAndShape = do
  combFn <- combinerClass outAssocAndShape
  return ("mkProduct", [tmplClass combFn])

-- Select<Pred, From>
selCall
  :: forall c e s t n m .
  (MonadCodeCheckpoint e s t n m,Traversable c,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> Prop (Rel (Expr (ShapeSym e s)))
  -> m Constr
selCall UnRev outSh _ = unionCall outSh
selCall UnFwd OutShape{oaIoAssoc=outAssoc} p = do
  propCls <- propCallClass =<< traverse3 (translateSym $ fmap swap outAssoc) p
  return ("mkSelect", [tmplClass propCls])


getQueriesFromClust
  :: (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => AnyCluster e s t n
  -> m [QNFQuery e s]
getQueriesFromClust clust = getNodeQnfN $ snd $ primaryNRef clust

-- | get the inputs, evalQueryEnv with them and copy the queryCall
-- stuff. The fact that we have a
clusterCall
  :: forall e s t n m .
  (MonadReader (ClusterConfig e s t n) m,MonadCodeCheckpoint e s t n m)
  => Direction
  -> AnyCluster e s t n
  -> m Constr
clusterCall dir c = do
  (ACPropagatorAssoc {acpaInOutAssoc = assoc}
    ,shapeClust) <- getValidClustPropagator c
  go shapeClust assoc c
  where
    go :: ShapeCluster NodeRef e s t n
       -> [(ShapeSym e s,ShapeSym e s)]
       -> AnyCluster e s t n
       -> m Constr
    go shapeClust assoc = \case
      JoinClustW c -> case shapeClust of
        JoinClustW pc -> go (BinClustW $ joinBinCluster pc) assoc
          $ BinClustW
          $ joinBinCluster c
        _ ->
          throwAStr $ "Expected join shapeclust but got: " ++ ashow shapeClust
      BinClustW BinClust {..} -> do
        -- The default shapes have been synchronized and are ready to
        -- go.
        outSyms <- case shapeClust of
          BinClustW BinClust {..} -> getDef' binClusterOut
          _ ->
            throwAStr $ "Expected bin shapeclust but got: " ++ ashow shapeClust
        op <- selectOp (ashow . (,assoc)) (checkBOp outSyms) binClusterOut
        l <- fullShape binClusterLeftIn
        r <- fullShape binClusterRightIn
        outShape <- fullShape binClusterOut
        case dir of
          ReverseTrigger -> evalQueryEnv (Identity outShape)
            $ bopQueryCall
              BinRev
              OutShape { oaIoAssoc = swap <$> assoc,oaShape = l }
              op
          ForwardTrigger -> evalQueryEnv (Tup2 l r)
            $ bopQueryCall
              BinFwd
              OutShape { oaIoAssoc = assoc,oaShape = outShape }
              op
      UnClustW UnClust {..} -> do
        (inSyms,outSyms) <- case shapeClust of
          UnClustW UnClust {..} ->
            (,) <$> getDef' unClusterIn <*> getDef' unClusterPrimaryOut
          _ ->
            throwAStr $ "Expected un shapeclust but got: " ++ ashow shapeClust
        op <- selectOp
          (ashow . (,assoc))
          (checkUOp inSyms outSyms)
          unClusterPrimaryOut
        inShape <- fullShape unClusterIn
        -- Remember we are making the primary one here
        primOutShape <- fullShape unClusterPrimaryOut
        case dir of
          ReverseTrigger -> do
            secOutShape <- fullShape unClusterSecondaryOut
            evalQueryEnv (Tup2 primOutShape secOutShape)
              $ uopQueryCall
              UnRev
              OutShape { oaIoAssoc = swap <$> assoc,oaShape = inShape }
              op
          ForwardTrigger -> evalQueryEnv (Identity inShape)
            $ uopQueryCall
            UnFwd
            OutShape { oaIoAssoc = assoc,oaShape = primOutShape }
            op
      NClustW (NClust r) -> throwCodeErr $ ForwardCreateSymbol r
      where
        getDef' :: HasCallStack
                => WMetaD x (WMetaD (Defaulting (QueryShape e s)) f) n
                -> m [ShapeSym e s]
        getDef' (WMetaD (_,WMetaD (d,_))) = case getDef d of
          Nothing
           -> throwAStr "Found empty defaulting in getValidClustPropagator"
          Just x -> return $ shapeAllSyms x
        selem sym syms = case shapeSymQnfName sym of
          NonSymbolName _ -> True
          _               -> sym `elem` syms
        checkUOp inSyms outSyms = \case
          QSel p -> all3 (`selem` outSyms) p
          QProj pr -> all (`selem` outSyms) (fst <$> pr)
            && all (`selem` inSyms) (toList3 pr)
          QGroup pr es -> all (`selem` outSyms) (fst <$> pr)
            && all (`selem` inSyms) (toList3 $ toList3 pr)
            && all (`selem` inSyms) (toList2 es)
          QSort es -> all2 (`selem` outSyms) es
          QLimit _ -> True
          QDrop _ -> True
        checkBOp outSyms = \case
          QJoin p          -> all3 (`selem` outSyms) p
          QLeftAntijoin p  -> all3 (`selem` outSyms) p
          QRightAntijoin p -> all3 (`selem` outSyms) p
          QProd            -> True
          QUnion           -> True
          QProjQuery       -> True
          QDistinct        -> True
        selectOp :: ((AShowV e,AShowV s) => [f (ShapeSym e s)] -> String)
                 -> (f (ShapeSym e s) -> Bool)
                 -> WMetaD [f (ShapeSym e s)] NodeRef n
                 -> m (f (ShapeSym e s))
        selectOp msg checkOp (WMetaD (ops,_)) = case filter checkOp ops of
          []   -> throwAStr $ "No matching operator on output: " ++ msg ops
          op:_ -> return op
        fullShape :: WMetaD a NodeRef n -> m (QueryShape e s)
        fullShape (WMetaD (_,ref)) =
          getNodeShape ref >>= maybe err return . getDefaultingFull
          where
            err =
              throwError $ fromString $ "No QueryShape for node" ++ ashow ref

bopQueryCall
  :: forall e s t n c m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => BinDir c
  -> OutShape e s
  -> BQOp (ShapeSym e s)
  -> m Constr
bopQueryCall dir outAssocAndShape = \case
  QJoin p          -> joinCall dir outAssocAndShape p
  QDistinct        -> throwAStr "QDistinct should have been optimized out."
  QProjQuery       -> throwAStr "QProjQuery should have been optimized out."
  QUnion           -> case dir of
    BinRev -> error "union does not have a reverse."
    BinFwd -> unionCall outAssocAndShape
  QProd            -> case dir of
    BinRev -> error "We do not deal with reverse products"
    BinFwd -> prodCall  outAssocAndShape
  QLeftAntijoin p  -> joinCall dir outAssocAndShape p
  QRightAntijoin p -> joinCall dir outAssocAndShape p

uopQueryCall
  :: forall e s t n c m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => UnDir c
  -> OutShape e s
  -> UQOp (ShapeSym e s)
  -> m Constr
uopQueryCall dir outSh op = case op of
  -- Remember that the symbols of selection refer to the out shape.
  QSel p -> selCall dir outSh p
  QGroup pr g -> case dir of
    UnRev -> error "Can't reverse trigger the group call"
    UnFwd -> groupCall pr g
  QProj pr -> projCall dir outSh pr
  QSort sr -> case dir of
    UnRev -> error "Cant reverse trigger sort"
    UnFwd -> sortCall outSh sr
  QLimit i -> limitCall dir outSh i
  QDrop i -> dropCall dir outSh i

opConstructor :: Constr
              -> IOFiles e s
              -> Maybe (CC.Expression CC.CodeSymbol)
opConstructor (name, tmpl) ioFiles = case constrArgs ioFiles of
  Just x  -> Just $ CC.FunctionAp name tmpl x
  Nothing -> Nothing

opStatements :: CC.Expression CC.CodeSymbol
             -> [CC.Statement CC.CodeSymbol]
opStatements ctor = [
  CC.DeclarationSt (CC.Declaration "operation" "auto") $ Just ctor
  , member "operation" "run" []
  , member "operation" "print_output" [CC.LiteralIntExpression 10]]
  where
    member :: CC.Expression CC.CodeSymbol
           -> CC.FunctionSymbol CC.CodeSymbol
           -> [CC.Expression CC.CodeSymbol]
           -> CC.Statement CC.CodeSymbol
    member exp mem arg = CC.ExpressionSt
                         $ CC.FunctionAp (CC.InstanceMember exp mem) [] arg

class ConstructorArg a where
  -- |Provide a value to get the c++ type (c++ types do not have to
  -- match haskell types). In some cases we don't have a value but we
  -- need a type.
  toConstrType :: Maybe a -> CC.Type CC.CodeSymbol
  toConstrArg :: a -> CC.Expression CC.CodeSymbol

instance ConstructorArg x => ConstructorArg (Identity x) where
  toConstrType = toConstrType . fmap runIdentity
  toConstrArg = toConstrArg . runIdentity
instance ConstructorArg a => ConstructorArg (Maybe (QueryShape e s), a) where
  toConstrType = toConstrType . fmap snd
  toConstrArg = toConstrArg . snd
instance ConstructorArg a => ConstructorArg (Maybe a) where
  toConstrType :: forall a . ConstructorArg a => Maybe (Maybe a) -> CC.Type CC.CodeSymbol
  toConstrType = \case
    Nothing -> constr "Nothing" (Nothing :: Maybe a)
    Just x  -> constr "Just" x
    where
      constr constrName constrArg = CC.ClassType
        (DS.singleton CC.CppConst)
        [CC.TypeArg $ toConstrType constrArg]
        (fromString constrName)

  toConstrArg :: forall a . ConstructorArg a => Maybe a -> CC.Expression CC.CodeSymbol
  toConstrArg = \case
    Just x  -> constr "Just" (Just x) [toConstrArg x]
    Nothing -> constr "Nothing" (Nothing :: Maybe a) []
    where
      constr :: String
             -> Maybe a
             -> [CC.Expression CC.CodeSymbol]
             -> CC.Expression CC.CodeSymbol
      constr constrName constrArg = CC.FunctionAp
        (fromString constrName)
        [CC.TypeArg $ toConstrType constrArg]

instance ConstructorArg FileSet where
  toConstrType = \case
    Nothing -> toConstrType (Nothing :: Maybe FilePath)
    Just (DataFile f) -> toConstrType $ Just f
    Just (DataAndSet f f') ->
      CC.ClassType
      (DS.singleton CC.CppConst)
      (CC.TypeArg . toConstrType . Just <$> [f, f'])
      "std::pair"
  toConstrArg = \case
    DataAndSet x y -> CC.FunctionAp "std::pair" (argType <$> [x, y])
                     $ toConstrArg <$> [x, y]
    DataFile x -> toConstrArg x
    where
      argType = CC.TypeArg . toConstrType . Just

instance ConstructorArg FilePath where
  toConstrType _ = CC.constString
  toConstrArg = CC.LiteralStringExpression

constrArgs' :: (ConstructorArg out, ConstructorArg inp) =>
              [out]
            -> [inp]
            -> [CC.Expression CC.CodeSymbol]
constrArgs' outs ins = (toConstrArg <$> outs) ++ (toConstrArg <$> ins)

constrArgs :: IOFiles e s -> Maybe [CC.Expression CC.CodeSymbol]
constrArgs ioFiles = constrArgs' outs <$> sequenceA inps'
  where
    (outs, inps') = toFiles ioFiles

constrArgsRevSide
  :: (ConstructorArg out,ConstructorArg inp)
  => (([Maybe (Maybe (QueryShape e s),FileSet)]
      ,[Maybe (Maybe (QueryShape e s),FilePath)])
      -> Maybe ([out],[inp]))
  -> IOFiles e s
  -> Maybe [CC.Expression CC.CodeSymbol]
constrArgsRevSide f = fmap (uncurry constrArgs') . f . toFiles

maybeFirst :: [Maybe a] -> Maybe [a]
maybeFirst =  \case
  [x,_] -> return <$> x
  _     -> Nothing
maybeSecond :: [Maybe a] -> Maybe [a]
maybeSecond =  \case
  [x,_] -> return <$> x
  _     -> Nothing
constrArgsRevLeft :: IOFiles e s -> Maybe [CC.Expression CC.CodeSymbol]
constrArgsRevLeft = constrArgsRevSide $ traverse maybeFirst
constrArgsRevRight :: IOFiles e s -> Maybe [CC.Expression CC.CodeSymbol]
constrArgsRevRight = constrArgsRevSide $ traverse maybeSecond
constrArgsRevRightStr :: IOFiles e s -> Maybe [CC.Expression CC.CodeSymbol]
constrArgsRevRightStr = constrArgsRevSide $ bitraverse sequenceA maybeFirst
constrArgsRevLeftStr :: IOFiles e s -> Maybe [CC.Expression CC.CodeSymbol]
constrArgsRevLeftStr = constrArgsRevSide $ bitraverse sequenceA maybeSecond

constrArgsRevUnary :: IOFiles e s -> Maybe [CC.Expression CC.CodeSymbol]
constrArgsRevUnary =
  fmap (uncurry constrArgs') . traverse sequenceA . swap . toFiles

constrBlock :: Constr -> IOFiles e s -> Maybe (CC.Statement CC.CodeSymbol)
constrBlock c iof = CC.Block . opStatements <$> opConstructor c iof
