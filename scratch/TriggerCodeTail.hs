-- This was at the tail of TriggerCode.h

#if 0
-- |Create code that triggers the query.
triggerCode
  :: forall e s t n m .
  (MonadReader (ClusterConfig e s t n) m
  ,MonadCodeCheckpoint e s t n m
  ,MonadSchemaScope Identity e s m)
  => AnyCluster e s t n
  -> IOFiles
  -> m (CC.Statement CC.CodeSymbol)
triggerCode clust ioFiles = do
  constr <- clusterCall clust
  case constrBlock constr ioFiles of
    Just x  -> return x
    Nothing -> do
      Identity query <- getQueries
      throwCodeErr $ MissingInputFile
        (clusterInputs clust, clusterOutputs clust) query


-- |Create code that triggers the query in reverse. The IOFiles
-- provided should already be reversed.
-- New implementation
revTriggerCode
  :: forall e s t n m .
  (MonadCodeCheckpoint e s t n m,MonadSchemaScope Identity e s m)
  => AnyCluster e s t n
  -> IOFiles
  -> m (CC.Statement CC.CodeSymbol)
revTriggerCode op ioFiles0 = fmap CC.Block $ do
  sch <- runIdentity . getQueries
  case op of
    Left o -> case o of
      QProd -> do
        stl <- grpStatements q $ constrArgsRevLeft ioFiles
        str <- grpStatements q' $ constrArgsRevRight ioFiles
        return $ stl ++ str
        where
          grpStatements :: QueryShape e s
                        -> Maybe [CC.Expression CC.CodeSymbol]
                        -> m [CC.Statement CC.CodeSymbol]
          grpStatements q = \case
            Nothing -> return []
            Just args -> do
              (name, tmpl) <- evalQueryEnv (Identity q) unProdCall
              return $ opStatements $ CC.FunctionAp name tmpl args
    QJoin _ -> do
      stl <- unJoinStatements q $ constrArgsRevLeftStr ioFiles
      str <- unJoinStatements q' $ constrArgsRevRightStr ioFiles
      return $ stl ++ str
        where
          unJoinStatements q = \case
            Nothing -> return []
            Just args -> do
              (name, tmpl) <- evalQueryEnv (Identity q) unJoinCall
              return $ opStatements $ CC.FunctionAp name tmpl args
    _ -> throwCodeErr $ UnsupportedReverse query
  Q1 o q -> evalQueryEnv (Identity q) $ case o of
    QProj pr -> case constrArgsRevUnary ioFiles of
      Nothing -> return []
      Just args -> do
        (name, tmpl) <- unProjCall pr
        return $ opStatements $ CC.FunctionAp name tmpl args
    QSel _ -> do
      (name, tmpl) <- unionCall
      return $ case constrArgsRevUnary ioFiles of
        Nothing   -> []
        Just args -> opStatements $ CC.FunctionAp name tmpl args
    _ -> throwCodeErr $ UnsupportedReverse query
  Q0 s -> throwAStr $ SymbolReverse s
#endif
