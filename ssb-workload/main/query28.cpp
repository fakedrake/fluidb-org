#include <array>

#include <string>

#include <codegen.hh>
class Record1 {
 public:
  Record1(unsigned __sym__0___45_8202918452351256080, fluidb_string<18> __sym__0__3217182456213118816, fluidb_string<9> __sym__0___45_587373445859874706, fluidb_string<9> __sym__0___45_8346128157561074223, unsigned __sym__0__2979561800187053807, unsigned __sym__0___45_899573490466067231, fluidb_string<7> __sym__0___45_2706395517471750200, unsigned __sym__0___45_6067709637706619093, unsigned __sym__0__1141311422513456040, unsigned __sym__0___45_5223888065355534256, unsigned __sym__0__2672047382496516406, unsigned __sym__0__4932263286160334513, fluidb_string<15> __sym__0__4463721534751519028, fluidb_string<2> __sym__0___45_4864066414314726068, fluidb_string<2> __sym__0__937958105294580029, fluidb_string<2> __sym__0___45_1982623509189166171, fluidb_string<2> __sym__0__4762673478997720679, unsigned __sym__0__7588001997570016652, int __sym__0___45_4747443848713997322, unsigned __sym__0___45_3257188284918960498, unsigned __sym__0__1617172947989036440, unsigned __sym__0___45_810220525799073153, unsigned __sym__0__8132341549271366762, fluidb_string<21> __sym__0__1394287910975083263, unsigned __sym__0__5644261439804640685, fluidb_string<10> __sym__0__9057335116627787790, unsigned __sym__0__2339428003903657470, double __sym__0__3371773490934440332, unsigned __sym__0___45_1286955585961260406, double __sym__0___45_1657469713282332988, unsigned __sym__0__816781160548213904, unsigned __sym__0__8401801716519894867, double __sym__0__5709933202258624457, fluidb_string<13> __sym__0__7900257115361061889) : sym__0___45_8202918452351256080(__sym__0___45_8202918452351256080), sym__0__3217182456213118816(__sym__0__3217182456213118816), sym__0___45_587373445859874706(__sym__0___45_587373445859874706), sym__0___45_8346128157561074223(__sym__0___45_8346128157561074223), sym__0__2979561800187053807(__sym__0__2979561800187053807), sym__0___45_899573490466067231(__sym__0___45_899573490466067231), sym__0___45_2706395517471750200(__sym__0___45_2706395517471750200), sym__0___45_6067709637706619093(__sym__0___45_6067709637706619093), sym__0__1141311422513456040(__sym__0__1141311422513456040), sym__0___45_5223888065355534256(__sym__0___45_5223888065355534256), sym__0__2672047382496516406(__sym__0__2672047382496516406), sym__0__4932263286160334513(__sym__0__4932263286160334513), sym__0__4463721534751519028(__sym__0__4463721534751519028), sym__0___45_4864066414314726068(__sym__0___45_4864066414314726068), sym__0__937958105294580029(__sym__0__937958105294580029), sym__0___45_1982623509189166171(__sym__0___45_1982623509189166171), sym__0__4762673478997720679(__sym__0__4762673478997720679), sym__0__7588001997570016652(__sym__0__7588001997570016652), sym__0___45_4747443848713997322(__sym__0___45_4747443848713997322), sym__0___45_3257188284918960498(__sym__0___45_3257188284918960498), sym__0__1617172947989036440(__sym__0__1617172947989036440), sym__0___45_810220525799073153(__sym__0___45_810220525799073153), sym__0__8132341549271366762(__sym__0__8132341549271366762), sym__0__1394287910975083263(__sym__0__1394287910975083263), sym__0__5644261439804640685(__sym__0__5644261439804640685), sym__0__9057335116627787790(__sym__0__9057335116627787790), sym__0__2339428003903657470(__sym__0__2339428003903657470), sym__0__3371773490934440332(__sym__0__3371773490934440332), sym__0___45_1286955585961260406(__sym__0___45_1286955585961260406), sym__0___45_1657469713282332988(__sym__0___45_1657469713282332988), sym__0__816781160548213904(__sym__0__816781160548213904), sym__0__8401801716519894867(__sym__0__8401801716519894867), sym__0__5709933202258624457(__sym__0__5709933202258624457), sym__0__7900257115361061889(__sym__0__7900257115361061889)
  {
  }
  Record1() 
  {
  }
  std::string show() const{
    std::stringstream o;
    o << sym__0___45_8202918452351256080 << " | " << arrToString(sym__0__3217182456213118816) << " | " << arrToString(sym__0___45_587373445859874706) << " | " << arrToString(sym__0___45_8346128157561074223) << " | " << sym__0__2979561800187053807 << " | " << sym__0___45_899573490466067231 << " | " << arrToString(sym__0___45_2706395517471750200) << " | " << sym__0___45_6067709637706619093 << " | " << sym__0__1141311422513456040 << " | " << sym__0___45_5223888065355534256 << " | " << sym__0__2672047382496516406 << " | " << sym__0__4932263286160334513 << " | " << arrToString(sym__0__4463721534751519028) << " | " << arrToString(sym__0___45_4864066414314726068) << " | " << arrToString(sym__0__937958105294580029) << " | " << arrToString(sym__0___45_1982623509189166171) << " | " << arrToString(sym__0__4762673478997720679) << " | " << sym__0__7588001997570016652 << " | " << sym__0___45_4747443848713997322 << " | " << sym__0___45_3257188284918960498 << " | " << sym__0__1617172947989036440 << " | " << sym__0___45_810220525799073153 << " | " << sym__0__8132341549271366762 << " | " << arrToString(sym__0__1394287910975083263) << " | " << sym__0__5644261439804640685 << " | " << arrToString(sym__0__9057335116627787790) << " | " << sym__0__2339428003903657470 << " | " << sym__0__3371773490934440332 << " | " << sym__0___45_1286955585961260406 << " | " << sym__0___45_1657469713282332988 << " | " << sym__0__816781160548213904 << " | " << sym__0__8401801716519894867 << " | " << sym__0__5709933202258624457 << " | " << arrToString(sym__0__7900257115361061889);
    return o.str();
  }
  bool operator <(const Record1& otherRec) const{
    return (otherRec.sym__0___45_8202918452351256080 < sym__0___45_8202918452351256080 && (otherRec.sym__0__3217182456213118816 < sym__0__3217182456213118816 && (otherRec.sym__0___45_587373445859874706 < sym__0___45_587373445859874706 && (otherRec.sym__0___45_8346128157561074223 < sym__0___45_8346128157561074223 && (otherRec.sym__0__2979561800187053807 < sym__0__2979561800187053807 && (otherRec.sym__0___45_899573490466067231 < sym__0___45_899573490466067231 && (otherRec.sym__0___45_2706395517471750200 < sym__0___45_2706395517471750200 && (otherRec.sym__0___45_6067709637706619093 < sym__0___45_6067709637706619093 && (otherRec.sym__0__1141311422513456040 < sym__0__1141311422513456040 && (otherRec.sym__0___45_5223888065355534256 < sym__0___45_5223888065355534256 && (otherRec.sym__0__2672047382496516406 < sym__0__2672047382496516406 && (otherRec.sym__0__4932263286160334513 < sym__0__4932263286160334513 && (otherRec.sym__0__4463721534751519028 < sym__0__4463721534751519028 && (otherRec.sym__0___45_4864066414314726068 < sym__0___45_4864066414314726068 && (otherRec.sym__0__937958105294580029 < sym__0__937958105294580029 && (otherRec.sym__0___45_1982623509189166171 < sym__0___45_1982623509189166171 && (otherRec.sym__0__4762673478997720679 < sym__0__4762673478997720679 && (otherRec.sym__0__7588001997570016652 < sym__0__7588001997570016652 && (otherRec.sym__0___45_4747443848713997322 < sym__0___45_4747443848713997322 && (otherRec.sym__0___45_3257188284918960498 < sym__0___45_3257188284918960498 && (otherRec.sym__0__1617172947989036440 < sym__0__1617172947989036440 && (otherRec.sym__0___45_810220525799073153 < sym__0___45_810220525799073153 && (otherRec.sym__0__8132341549271366762 < sym__0__8132341549271366762 && (otherRec.sym__0__1394287910975083263 < sym__0__1394287910975083263 && (otherRec.sym__0__5644261439804640685 < sym__0__5644261439804640685 && (otherRec.sym__0__9057335116627787790 < sym__0__9057335116627787790 && (otherRec.sym__0__2339428003903657470 < sym__0__2339428003903657470 && (otherRec.sym__0__3371773490934440332 < sym__0__3371773490934440332 && (otherRec.sym__0___45_1286955585961260406 < sym__0___45_1286955585961260406 && (otherRec.sym__0___45_1657469713282332988 < sym__0___45_1657469713282332988 && (otherRec.sym__0__816781160548213904 < sym__0__816781160548213904 && (otherRec.sym__0__8401801716519894867 < sym__0__8401801716519894867 && (otherRec.sym__0__5709933202258624457 < sym__0__5709933202258624457 && otherRec.sym__0__7900257115361061889 < sym__0__7900257115361061889)))))))))))))))))))))))))))))))));
  }
  bool operator ==(const Record1& otherRec) const{
    return (otherRec.sym__0___45_8202918452351256080 == sym__0___45_8202918452351256080 && (otherRec.sym__0__3217182456213118816 == sym__0__3217182456213118816 && (otherRec.sym__0___45_587373445859874706 == sym__0___45_587373445859874706 && (otherRec.sym__0___45_8346128157561074223 == sym__0___45_8346128157561074223 && (otherRec.sym__0__2979561800187053807 == sym__0__2979561800187053807 && (otherRec.sym__0___45_899573490466067231 == sym__0___45_899573490466067231 && (otherRec.sym__0___45_2706395517471750200 == sym__0___45_2706395517471750200 && (otherRec.sym__0___45_6067709637706619093 == sym__0___45_6067709637706619093 && (otherRec.sym__0__1141311422513456040 == sym__0__1141311422513456040 && (otherRec.sym__0___45_5223888065355534256 == sym__0___45_5223888065355534256 && (otherRec.sym__0__2672047382496516406 == sym__0__2672047382496516406 && (otherRec.sym__0__4932263286160334513 == sym__0__4932263286160334513 && (otherRec.sym__0__4463721534751519028 == sym__0__4463721534751519028 && (otherRec.sym__0___45_4864066414314726068 == sym__0___45_4864066414314726068 && (otherRec.sym__0__937958105294580029 == sym__0__937958105294580029 && (otherRec.sym__0___45_1982623509189166171 == sym__0___45_1982623509189166171 && (otherRec.sym__0__4762673478997720679 == sym__0__4762673478997720679 && (otherRec.sym__0__7588001997570016652 == sym__0__7588001997570016652 && (otherRec.sym__0___45_4747443848713997322 == sym__0___45_4747443848713997322 && (otherRec.sym__0___45_3257188284918960498 == sym__0___45_3257188284918960498 && (otherRec.sym__0__1617172947989036440 == sym__0__1617172947989036440 && (otherRec.sym__0___45_810220525799073153 == sym__0___45_810220525799073153 && (otherRec.sym__0__8132341549271366762 == sym__0__8132341549271366762 && (otherRec.sym__0__1394287910975083263 == sym__0__1394287910975083263 && (otherRec.sym__0__5644261439804640685 == sym__0__5644261439804640685 && (otherRec.sym__0__9057335116627787790 == sym__0__9057335116627787790 && (otherRec.sym__0__2339428003903657470 == sym__0__2339428003903657470 && (otherRec.sym__0__3371773490934440332 == sym__0__3371773490934440332 && (otherRec.sym__0___45_1286955585961260406 == sym__0___45_1286955585961260406 && (otherRec.sym__0___45_1657469713282332988 == sym__0___45_1657469713282332988 && (otherRec.sym__0__816781160548213904 == sym__0__816781160548213904 && (otherRec.sym__0__8401801716519894867 == sym__0__8401801716519894867 && (otherRec.sym__0__5709933202258624457 == sym__0__5709933202258624457 && otherRec.sym__0__7900257115361061889 == sym__0__7900257115361061889)))))))))))))))))))))))))))))))));
  }
  bool operator !=(const Record1& otherRec) const{
    return (otherRec.sym__0___45_8202918452351256080 != sym__0___45_8202918452351256080 || (otherRec.sym__0__3217182456213118816 != sym__0__3217182456213118816 || (otherRec.sym__0___45_587373445859874706 != sym__0___45_587373445859874706 || (otherRec.sym__0___45_8346128157561074223 != sym__0___45_8346128157561074223 || (otherRec.sym__0__2979561800187053807 != sym__0__2979561800187053807 || (otherRec.sym__0___45_899573490466067231 != sym__0___45_899573490466067231 || (otherRec.sym__0___45_2706395517471750200 != sym__0___45_2706395517471750200 || (otherRec.sym__0___45_6067709637706619093 != sym__0___45_6067709637706619093 || (otherRec.sym__0__1141311422513456040 != sym__0__1141311422513456040 || (otherRec.sym__0___45_5223888065355534256 != sym__0___45_5223888065355534256 || (otherRec.sym__0__2672047382496516406 != sym__0__2672047382496516406 || (otherRec.sym__0__4932263286160334513 != sym__0__4932263286160334513 || (otherRec.sym__0__4463721534751519028 != sym__0__4463721534751519028 || (otherRec.sym__0___45_4864066414314726068 != sym__0___45_4864066414314726068 || (otherRec.sym__0__937958105294580029 != sym__0__937958105294580029 || (otherRec.sym__0___45_1982623509189166171 != sym__0___45_1982623509189166171 || (otherRec.sym__0__4762673478997720679 != sym__0__4762673478997720679 || (otherRec.sym__0__7588001997570016652 != sym__0__7588001997570016652 || (otherRec.sym__0___45_4747443848713997322 != sym__0___45_4747443848713997322 || (otherRec.sym__0___45_3257188284918960498 != sym__0___45_3257188284918960498 || (otherRec.sym__0__1617172947989036440 != sym__0__1617172947989036440 || (otherRec.sym__0___45_810220525799073153 != sym__0___45_810220525799073153 || (otherRec.sym__0__8132341549271366762 != sym__0__8132341549271366762 || (otherRec.sym__0__1394287910975083263 != sym__0__1394287910975083263 || (otherRec.sym__0__5644261439804640685 != sym__0__5644261439804640685 || (otherRec.sym__0__9057335116627787790 != sym__0__9057335116627787790 || (otherRec.sym__0__2339428003903657470 != sym__0__2339428003903657470 || (otherRec.sym__0__3371773490934440332 != sym__0__3371773490934440332 || (otherRec.sym__0___45_1286955585961260406 != sym__0___45_1286955585961260406 || (otherRec.sym__0___45_1657469713282332988 != sym__0___45_1657469713282332988 || (otherRec.sym__0__816781160548213904 != sym__0__816781160548213904 || (otherRec.sym__0__8401801716519894867 != sym__0__8401801716519894867 || (otherRec.sym__0__5709933202258624457 != sym__0__5709933202258624457 || otherRec.sym__0__7900257115361061889 != sym__0__7900257115361061889)))))))))))))))))))))))))))))))));
  }
  unsigned sym__0___45_8202918452351256080;
  fluidb_string<18> sym__0__3217182456213118816;
  fluidb_string<9> sym__0___45_587373445859874706;
  fluidb_string<9> sym__0___45_8346128157561074223;
  unsigned sym__0__2979561800187053807;
  unsigned sym__0___45_899573490466067231;
  fluidb_string<7> sym__0___45_2706395517471750200;
  unsigned sym__0___45_6067709637706619093;
  unsigned sym__0__1141311422513456040;
  unsigned sym__0___45_5223888065355534256;
  unsigned sym__0__2672047382496516406;
  unsigned sym__0__4932263286160334513;
  fluidb_string<15> sym__0__4463721534751519028;
  fluidb_string<2> sym__0___45_4864066414314726068;
  fluidb_string<2> sym__0__937958105294580029;
  fluidb_string<2> sym__0___45_1982623509189166171;
  fluidb_string<2> sym__0__4762673478997720679;
  unsigned sym__0__7588001997570016652;
  int sym__0___45_4747443848713997322;
  unsigned sym__0___45_3257188284918960498;
  unsigned sym__0__1617172947989036440;
  unsigned sym__0___45_810220525799073153;
  unsigned sym__0__8132341549271366762;
  fluidb_string<21> sym__0__1394287910975083263;
  unsigned sym__0__5644261439804640685;
  fluidb_string<10> sym__0__9057335116627787790;
  unsigned sym__0__2339428003903657470;
  double sym__0__3371773490934440332;
  unsigned sym__0___45_1286955585961260406;
  double sym__0___45_1657469713282332988;
  unsigned sym__0__816781160548213904;
  unsigned sym__0__8401801716519894867;
  double sym__0__5709933202258624457;
  fluidb_string<13> sym__0__7900257115361061889;
 private:
};


class Record4 {
 public:
  Record4(double __sym__0__5918592799609927687) : sym__0__5918592799609927687(__sym__0__5918592799609927687)
  {
  }
  Record4() 
  {
  }
  std::string show() const{
    std::stringstream o;
    o << sym__0__5918592799609927687;
    return o.str();
  }
  bool operator <(const Record4& otherRec) const{
    return otherRec.sym__0__5918592799609927687 < sym__0__5918592799609927687;
  }
  bool operator ==(const Record4& otherRec) const{
    return otherRec.sym__0__5918592799609927687 == sym__0__5918592799609927687;
  }
  bool operator !=(const Record4& otherRec) const{
    return otherRec.sym__0__5918592799609927687 != sym__0__5918592799609927687;
  }
  double sym__0__5918592799609927687;
 private:
};


class Record6 {
 public:
  Record6(unsigned __sym__0__3065136345967461703, fluidb_string<18> __sym__0___45_255809025746352457, fluidb_string<9> __sym__0___45_3052415535374083467, fluidb_string<9> __sym__0__7276052820189536666, unsigned __sym__0___45_3217408013537744440, unsigned __sym__0___45_4647437684924983382, fluidb_string<7> __sym__0__1638410156777433327, unsigned __sym__0__3646817083878080772, unsigned __sym__0___45_7985964654281987185, unsigned __sym__0__2636191205359665255, unsigned __sym__0___45_1092205188953991859, unsigned __sym__0___45_3994277950988897606, fluidb_string<15> __sym__0___45_2337723089217712293, fluidb_string<2> __sym__0___45_5794278497288405693, fluidb_string<2> __sym__0___45_5354400952585076442, fluidb_string<2> __sym__0___45_2972406684929314, fluidb_string<2> __sym__0__7789453781397041744, unsigned __sym__0___45_1389453690600897405, int __sym__0___45_2147362746752783603, unsigned __sym__0__8028023425137563477, unsigned __sym__0___45_1433387869147453761, unsigned __sym__0__2404722455036460120, unsigned __sym__0__881714021986804641, fluidb_string<21> __sym__0__6496474286921867736, unsigned __sym__0__6664309959799804246, fluidb_string<10> __sym__0___45_7331603023632415275, unsigned __sym__0___45_4506307731373530363, double __sym__0___45_7602287447917107069, unsigned __sym__0___45_6568809649648144767, double __sym__0__3413230238112622827, unsigned __sym__0___45_484365620635598937, unsigned __sym__0___45_5227352489991704484, double __sym__0__571588908207363490, fluidb_string<13> __sym__0___45_2012622824227713782) : sym__0__3065136345967461703(__sym__0__3065136345967461703), sym__0___45_255809025746352457(__sym__0___45_255809025746352457), sym__0___45_3052415535374083467(__sym__0___45_3052415535374083467), sym__0__7276052820189536666(__sym__0__7276052820189536666), sym__0___45_3217408013537744440(__sym__0___45_3217408013537744440), sym__0___45_4647437684924983382(__sym__0___45_4647437684924983382), sym__0__1638410156777433327(__sym__0__1638410156777433327), sym__0__3646817083878080772(__sym__0__3646817083878080772), sym__0___45_7985964654281987185(__sym__0___45_7985964654281987185), sym__0__2636191205359665255(__sym__0__2636191205359665255), sym__0___45_1092205188953991859(__sym__0___45_1092205188953991859), sym__0___45_3994277950988897606(__sym__0___45_3994277950988897606), sym__0___45_2337723089217712293(__sym__0___45_2337723089217712293), sym__0___45_5794278497288405693(__sym__0___45_5794278497288405693), sym__0___45_5354400952585076442(__sym__0___45_5354400952585076442), sym__0___45_2972406684929314(__sym__0___45_2972406684929314), sym__0__7789453781397041744(__sym__0__7789453781397041744), sym__0___45_1389453690600897405(__sym__0___45_1389453690600897405), sym__0___45_2147362746752783603(__sym__0___45_2147362746752783603), sym__0__8028023425137563477(__sym__0__8028023425137563477), sym__0___45_1433387869147453761(__sym__0___45_1433387869147453761), sym__0__2404722455036460120(__sym__0__2404722455036460120), sym__0__881714021986804641(__sym__0__881714021986804641), sym__0__6496474286921867736(__sym__0__6496474286921867736), sym__0__6664309959799804246(__sym__0__6664309959799804246), sym__0___45_7331603023632415275(__sym__0___45_7331603023632415275), sym__0___45_4506307731373530363(__sym__0___45_4506307731373530363), sym__0___45_7602287447917107069(__sym__0___45_7602287447917107069), sym__0___45_6568809649648144767(__sym__0___45_6568809649648144767), sym__0__3413230238112622827(__sym__0__3413230238112622827), sym__0___45_484365620635598937(__sym__0___45_484365620635598937), sym__0___45_5227352489991704484(__sym__0___45_5227352489991704484), sym__0__571588908207363490(__sym__0__571588908207363490), sym__0___45_2012622824227713782(__sym__0___45_2012622824227713782)
  {
  }
  Record6() 
  {
  }
  std::string show() const{
    std::stringstream o;
    o << sym__0__3065136345967461703 << " | " << arrToString(sym__0___45_255809025746352457) << " | " << arrToString(sym__0___45_3052415535374083467) << " | " << arrToString(sym__0__7276052820189536666) << " | " << sym__0___45_3217408013537744440 << " | " << sym__0___45_4647437684924983382 << " | " << arrToString(sym__0__1638410156777433327) << " | " << sym__0__3646817083878080772 << " | " << sym__0___45_7985964654281987185 << " | " << sym__0__2636191205359665255 << " | " << sym__0___45_1092205188953991859 << " | " << sym__0___45_3994277950988897606 << " | " << arrToString(sym__0___45_2337723089217712293) << " | " << arrToString(sym__0___45_5794278497288405693) << " | " << arrToString(sym__0___45_5354400952585076442) << " | " << arrToString(sym__0___45_2972406684929314) << " | " << arrToString(sym__0__7789453781397041744) << " | " << sym__0___45_1389453690600897405 << " | " << sym__0___45_2147362746752783603 << " | " << sym__0__8028023425137563477 << " | " << sym__0___45_1433387869147453761 << " | " << sym__0__2404722455036460120 << " | " << sym__0__881714021986804641 << " | " << arrToString(sym__0__6496474286921867736) << " | " << sym__0__6664309959799804246 << " | " << arrToString(sym__0___45_7331603023632415275) << " | " << sym__0___45_4506307731373530363 << " | " << sym__0___45_7602287447917107069 << " | " << sym__0___45_6568809649648144767 << " | " << sym__0__3413230238112622827 << " | " << sym__0___45_484365620635598937 << " | " << sym__0___45_5227352489991704484 << " | " << sym__0__571588908207363490 << " | " << arrToString(sym__0___45_2012622824227713782);
    return o.str();
  }
  bool operator <(const Record6& otherRec) const{
    return (otherRec.sym__0__3065136345967461703 < sym__0__3065136345967461703 && (otherRec.sym__0___45_255809025746352457 < sym__0___45_255809025746352457 && (otherRec.sym__0___45_3052415535374083467 < sym__0___45_3052415535374083467 && (otherRec.sym__0__7276052820189536666 < sym__0__7276052820189536666 && (otherRec.sym__0___45_3217408013537744440 < sym__0___45_3217408013537744440 && (otherRec.sym__0___45_4647437684924983382 < sym__0___45_4647437684924983382 && (otherRec.sym__0__1638410156777433327 < sym__0__1638410156777433327 && (otherRec.sym__0__3646817083878080772 < sym__0__3646817083878080772 && (otherRec.sym__0___45_7985964654281987185 < sym__0___45_7985964654281987185 && (otherRec.sym__0__2636191205359665255 < sym__0__2636191205359665255 && (otherRec.sym__0___45_1092205188953991859 < sym__0___45_1092205188953991859 && (otherRec.sym__0___45_3994277950988897606 < sym__0___45_3994277950988897606 && (otherRec.sym__0___45_2337723089217712293 < sym__0___45_2337723089217712293 && (otherRec.sym__0___45_5794278497288405693 < sym__0___45_5794278497288405693 && (otherRec.sym__0___45_5354400952585076442 < sym__0___45_5354400952585076442 && (otherRec.sym__0___45_2972406684929314 < sym__0___45_2972406684929314 && (otherRec.sym__0__7789453781397041744 < sym__0__7789453781397041744 && (otherRec.sym__0___45_1389453690600897405 < sym__0___45_1389453690600897405 && (otherRec.sym__0___45_2147362746752783603 < sym__0___45_2147362746752783603 && (otherRec.sym__0__8028023425137563477 < sym__0__8028023425137563477 && (otherRec.sym__0___45_1433387869147453761 < sym__0___45_1433387869147453761 && (otherRec.sym__0__2404722455036460120 < sym__0__2404722455036460120 && (otherRec.sym__0__881714021986804641 < sym__0__881714021986804641 && (otherRec.sym__0__6496474286921867736 < sym__0__6496474286921867736 && (otherRec.sym__0__6664309959799804246 < sym__0__6664309959799804246 && (otherRec.sym__0___45_7331603023632415275 < sym__0___45_7331603023632415275 && (otherRec.sym__0___45_4506307731373530363 < sym__0___45_4506307731373530363 && (otherRec.sym__0___45_7602287447917107069 < sym__0___45_7602287447917107069 && (otherRec.sym__0___45_6568809649648144767 < sym__0___45_6568809649648144767 && (otherRec.sym__0__3413230238112622827 < sym__0__3413230238112622827 && (otherRec.sym__0___45_484365620635598937 < sym__0___45_484365620635598937 && (otherRec.sym__0___45_5227352489991704484 < sym__0___45_5227352489991704484 && (otherRec.sym__0__571588908207363490 < sym__0__571588908207363490 && otherRec.sym__0___45_2012622824227713782 < sym__0___45_2012622824227713782)))))))))))))))))))))))))))))))));
  }
  bool operator ==(const Record6& otherRec) const{
    return (otherRec.sym__0__3065136345967461703 == sym__0__3065136345967461703 && (otherRec.sym__0___45_255809025746352457 == sym__0___45_255809025746352457 && (otherRec.sym__0___45_3052415535374083467 == sym__0___45_3052415535374083467 && (otherRec.sym__0__7276052820189536666 == sym__0__7276052820189536666 && (otherRec.sym__0___45_3217408013537744440 == sym__0___45_3217408013537744440 && (otherRec.sym__0___45_4647437684924983382 == sym__0___45_4647437684924983382 && (otherRec.sym__0__1638410156777433327 == sym__0__1638410156777433327 && (otherRec.sym__0__3646817083878080772 == sym__0__3646817083878080772 && (otherRec.sym__0___45_7985964654281987185 == sym__0___45_7985964654281987185 && (otherRec.sym__0__2636191205359665255 == sym__0__2636191205359665255 && (otherRec.sym__0___45_1092205188953991859 == sym__0___45_1092205188953991859 && (otherRec.sym__0___45_3994277950988897606 == sym__0___45_3994277950988897606 && (otherRec.sym__0___45_2337723089217712293 == sym__0___45_2337723089217712293 && (otherRec.sym__0___45_5794278497288405693 == sym__0___45_5794278497288405693 && (otherRec.sym__0___45_5354400952585076442 == sym__0___45_5354400952585076442 && (otherRec.sym__0___45_2972406684929314 == sym__0___45_2972406684929314 && (otherRec.sym__0__7789453781397041744 == sym__0__7789453781397041744 && (otherRec.sym__0___45_1389453690600897405 == sym__0___45_1389453690600897405 && (otherRec.sym__0___45_2147362746752783603 == sym__0___45_2147362746752783603 && (otherRec.sym__0__8028023425137563477 == sym__0__8028023425137563477 && (otherRec.sym__0___45_1433387869147453761 == sym__0___45_1433387869147453761 && (otherRec.sym__0__2404722455036460120 == sym__0__2404722455036460120 && (otherRec.sym__0__881714021986804641 == sym__0__881714021986804641 && (otherRec.sym__0__6496474286921867736 == sym__0__6496474286921867736 && (otherRec.sym__0__6664309959799804246 == sym__0__6664309959799804246 && (otherRec.sym__0___45_7331603023632415275 == sym__0___45_7331603023632415275 && (otherRec.sym__0___45_4506307731373530363 == sym__0___45_4506307731373530363 && (otherRec.sym__0___45_7602287447917107069 == sym__0___45_7602287447917107069 && (otherRec.sym__0___45_6568809649648144767 == sym__0___45_6568809649648144767 && (otherRec.sym__0__3413230238112622827 == sym__0__3413230238112622827 && (otherRec.sym__0___45_484365620635598937 == sym__0___45_484365620635598937 && (otherRec.sym__0___45_5227352489991704484 == sym__0___45_5227352489991704484 && (otherRec.sym__0__571588908207363490 == sym__0__571588908207363490 && otherRec.sym__0___45_2012622824227713782 == sym__0___45_2012622824227713782)))))))))))))))))))))))))))))))));
  }
  bool operator !=(const Record6& otherRec) const{
    return (otherRec.sym__0__3065136345967461703 != sym__0__3065136345967461703 || (otherRec.sym__0___45_255809025746352457 != sym__0___45_255809025746352457 || (otherRec.sym__0___45_3052415535374083467 != sym__0___45_3052415535374083467 || (otherRec.sym__0__7276052820189536666 != sym__0__7276052820189536666 || (otherRec.sym__0___45_3217408013537744440 != sym__0___45_3217408013537744440 || (otherRec.sym__0___45_4647437684924983382 != sym__0___45_4647437684924983382 || (otherRec.sym__0__1638410156777433327 != sym__0__1638410156777433327 || (otherRec.sym__0__3646817083878080772 != sym__0__3646817083878080772 || (otherRec.sym__0___45_7985964654281987185 != sym__0___45_7985964654281987185 || (otherRec.sym__0__2636191205359665255 != sym__0__2636191205359665255 || (otherRec.sym__0___45_1092205188953991859 != sym__0___45_1092205188953991859 || (otherRec.sym__0___45_3994277950988897606 != sym__0___45_3994277950988897606 || (otherRec.sym__0___45_2337723089217712293 != sym__0___45_2337723089217712293 || (otherRec.sym__0___45_5794278497288405693 != sym__0___45_5794278497288405693 || (otherRec.sym__0___45_5354400952585076442 != sym__0___45_5354400952585076442 || (otherRec.sym__0___45_2972406684929314 != sym__0___45_2972406684929314 || (otherRec.sym__0__7789453781397041744 != sym__0__7789453781397041744 || (otherRec.sym__0___45_1389453690600897405 != sym__0___45_1389453690600897405 || (otherRec.sym__0___45_2147362746752783603 != sym__0___45_2147362746752783603 || (otherRec.sym__0__8028023425137563477 != sym__0__8028023425137563477 || (otherRec.sym__0___45_1433387869147453761 != sym__0___45_1433387869147453761 || (otherRec.sym__0__2404722455036460120 != sym__0__2404722455036460120 || (otherRec.sym__0__881714021986804641 != sym__0__881714021986804641 || (otherRec.sym__0__6496474286921867736 != sym__0__6496474286921867736 || (otherRec.sym__0__6664309959799804246 != sym__0__6664309959799804246 || (otherRec.sym__0___45_7331603023632415275 != sym__0___45_7331603023632415275 || (otherRec.sym__0___45_4506307731373530363 != sym__0___45_4506307731373530363 || (otherRec.sym__0___45_7602287447917107069 != sym__0___45_7602287447917107069 || (otherRec.sym__0___45_6568809649648144767 != sym__0___45_6568809649648144767 || (otherRec.sym__0__3413230238112622827 != sym__0__3413230238112622827 || (otherRec.sym__0___45_484365620635598937 != sym__0___45_484365620635598937 || (otherRec.sym__0___45_5227352489991704484 != sym__0___45_5227352489991704484 || (otherRec.sym__0__571588908207363490 != sym__0__571588908207363490 || otherRec.sym__0___45_2012622824227713782 != sym__0___45_2012622824227713782)))))))))))))))))))))))))))))))));
  }
  unsigned sym__0__3065136345967461703;
  fluidb_string<18> sym__0___45_255809025746352457;
  fluidb_string<9> sym__0___45_3052415535374083467;
  fluidb_string<9> sym__0__7276052820189536666;
  unsigned sym__0___45_3217408013537744440;
  unsigned sym__0___45_4647437684924983382;
  fluidb_string<7> sym__0__1638410156777433327;
  unsigned sym__0__3646817083878080772;
  unsigned sym__0___45_7985964654281987185;
  unsigned sym__0__2636191205359665255;
  unsigned sym__0___45_1092205188953991859;
  unsigned sym__0___45_3994277950988897606;
  fluidb_string<15> sym__0___45_2337723089217712293;
  fluidb_string<2> sym__0___45_5794278497288405693;
  fluidb_string<2> sym__0___45_5354400952585076442;
  fluidb_string<2> sym__0___45_2972406684929314;
  fluidb_string<2> sym__0__7789453781397041744;
  unsigned sym__0___45_1389453690600897405;
  int sym__0___45_2147362746752783603;
  unsigned sym__0__8028023425137563477;
  unsigned sym__0___45_1433387869147453761;
  unsigned sym__0__2404722455036460120;
  unsigned sym__0__881714021986804641;
  fluidb_string<21> sym__0__6496474286921867736;
  unsigned sym__0__6664309959799804246;
  fluidb_string<10> sym__0___45_7331603023632415275;
  unsigned sym__0___45_4506307731373530363;
  double sym__0___45_7602287447917107069;
  unsigned sym__0___45_6568809649648144767;
  double sym__0__3413230238112622827;
  unsigned sym__0___45_484365620635598937;
  unsigned sym__0___45_5227352489991704484;
  double sym__0__571588908207363490;
  fluidb_string<13> sym__0___45_2012622824227713782;
 private:
};


class CallableClass7 {
 public:
  Record4 operator()(const Record6& record3) {
    return Record4(vAggrSum5(record3.sym__0___45_7602287447917107069 * record3.sym__0__3413230238112622827));
  }
  typedef Record4 Codomain;
  typedef Record6 Domain0;
 private:
  AggrSum<double> vAggrSum5;
};


class CallableClass2 {
 public:
  bool operator()(const Record1& record0) {
    return ((((5 <= record0.sym__0___45_1657469713282332988) && (record0.sym__0___45_1657469713282332988 <= 7)) && (26 <= record0.sym__0__2339428003903657470)) && (record0.sym__0__2339428003903657470 <= 35));
  }
  typedef bool Codomain;
  typedef Record1 Domain0;
 private:
};






int main() {
  // Delete: (Q1 
  //   (QGroup 
  //     [
  //      (
  //        ESym "revenue",
  //        E0 
  //           (NAggr 
  //             AggrSum 
  //             (E2 EMul 
  //                 (E0 (ESym "lo_extendedprice")) 
  //                 (E0 (ESym "lo_discount"))))
  //       )
  //     ] 
  //     []) 
  //   (S 
  //     (P2 
  //       PAnd 
  //       (P2 
  //         PAnd 
  //         (P2 
  //           PAnd 
  //           (P0 
  //             (R2 RLe (R0 (E0 (EInt 4))) (R0 (E0 (ESym "lo_discount"))))) 
  //           (P0 
  //             (R2 RLe (R0 (E0 (ESym "lo_discount"))) (R0 (E0 (EInt 6)))))) 
  //         (P0 (R2 RLe (R0 (E0 (EInt 26))) (R0 (E0 (ESym "lo_quantity")))))) 
  //       (P0 (R2 RLe (R0 (E0 (ESym "lo_quantity"))) (R0 (E0 (EInt 35)))))) 
  //     (J 
  //       (P0 
  //         (R2 REq 
  //             (R0 (E0 (ESym "lo_orderdate"))) 
  //             (R0 (E0 (ESym "d_datekey"))))) 
  //       (S 
  //         (P0 
  //           (R2 REq 
  //               (R0 (E0 (ESym "d_yearmonthnum"))) 
  //               (R0 (E0 (EInt 199401))))) 
  //         (Q0 (TSymbol "date"))) 
  std::cout << "Delete: (Q1 \n  (QGroup \n    [\n     (\n       ESym \"revenue\",\n       E0 \n          (NAggr \n            AggrSum \n            (E2 EMul \n                (E0 (ESym \"lo_extendedprice\")) \n                (E0 (ESym \"lo_discount\"))))\n      )\n    ] \n    []) \n  (S \n    (P2 \n      PAnd \n      (P2 \n        PAnd \n        (P2 \n          PAnd \n          (P0 \n            (R2 RLe (R0 (E0 (EInt 4))) (R0 (E0 (ESym \"lo_discount\"))))) \n          (P0 \n            (R2 RLe (R0 (E0 (ESym \"lo_discount\"))) (R0 (E0 (EInt 6)))))) \n        (P0 (R2 RLe (R0 (E0 (EInt 26))) (R0 (E0 (ESym \"lo_quantity\")))))) \n      (P0 (R2 RLe (R0 (E0 (ESym \"lo_quantity\"))) (R0 (E0 (EInt 35)))))) \n    (J \n      (P0 \n        (R2 REq \n            (R0 (E0 (ESym \"lo_orderdate\"))) \n            (R0 (E0 (ESym \"d_datekey\"))))) \n      (S \n        (P0 \n          (R2 REq \n              (R0 (E0 (ESym \"d_yearmonthnum\"))) \n              (R0 (E0 (EInt 199401))))) \n        (Q0 (TSymbol \"date\"))) \n      (Q0 (TSymbol \"lineorder\")))))" << std::endl;
  deleteFile("/tmp/fluidb_store/data14.dat");
  // Delete: (S 
  //   (P2 
  //     PAnd 
  //     (P2 
  //       PAnd 
  //       (P2 PAnd 
  //           (P0 (R2 RLe (R0 (E0 (EInt 4))) (R0 (E0 (ESym "lo_discount"))))) 
  //           (P0 (R2 RLe (R0 (E0 (ESym "lo_discount"))) (R0 (E0 (EInt 6)))))) 
  //       (P0 (R2 RLe (R0 (E0 (EInt 26))) (R0 (E0 (ESym "lo_quantity")))))) 
  //     (P0 (R2 RLe (R0 (E0 (ESym "lo_quantity"))) (R0 (E0 (EInt 35)))))) 
  //   (J 
  //     (P0 
  //       (R2 REq 
  //           (R0 (E0 (ESym "lo_orderdate"))) 
  //           (R0 (E0 (ESym "d_datekey"))))) 
  //     (S 
  //       (P0 
  //         (R2 REq 
  //             (R0 (E0 (ESym "d_yearmonthnum"))) 
  //             (R0 (E0 (EInt 199401))))) 
  //       (Q0 (TSymbol "date"))) 
  std::cout << "Delete: (S \n  (P2 \n    PAnd \n    (P2 \n      PAnd \n      (P2 PAnd \n          (P0 (R2 RLe (R0 (E0 (EInt 4))) (R0 (E0 (ESym \"lo_discount\"))))) \n          (P0 (R2 RLe (R0 (E0 (ESym \"lo_discount\"))) (R0 (E0 (EInt 6)))))) \n      (P0 (R2 RLe (R0 (E0 (EInt 26))) (R0 (E0 (ESym \"lo_quantity\")))))) \n    (P0 (R2 RLe (R0 (E0 (ESym \"lo_quantity\"))) (R0 (E0 (EInt 35)))))) \n  (J \n    (P0 \n      (R2 REq \n          (R0 (E0 (ESym \"lo_orderdate\"))) \n          (R0 (E0 (ESym \"d_datekey\"))))) \n    (S \n      (P0 \n        (R2 REq \n            (R0 (E0 (ESym \"d_yearmonthnum\"))) \n            (R0 (E0 (EInt 199401))))) \n      (Q0 (TSymbol \"date\"))) \n    (Q0 (TSymbol \"lineorder\"))))" << std::endl;
  deleteFile("/tmp/fluidb_store/data12.dat");
  // ForwardTrigger: (Just 
  //   (Right 
  //     [
  //      QSel 
  //         (P2 
  //           PAnd 
  //           (P2 
  //             PAnd 
  //             (P2 
  //               PAnd 
  //               (P0 
  //                 (R2 RLe 
  //                     (R0 (E0 (EInt 5))) 
  //                     (R0 (E0 (ESym "lo_discount"))))) 
  //               (P0 
  //                 (R2 RLe 
  //                     (R0 (E0 (ESym "lo_discount"))) 
  //                     (R0 (E0 (EInt 7)))))) 
  //             (P0 
  //               (R2 RLe 
  //                   (R0 (E0 (EInt 26))) 
  //                   (R0 (E0 (ESym "lo_quantity")))))) 
  //           (P0 
  //             (R2 RLe (R0 (E0 (ESym "lo_quantity"))) (R0 (E0 (EInt 35))))))
  std::cout << "ForwardTrigger: (Just \n  (Right \n    [\n     QSel \n        (P2 \n          PAnd \n          (P2 \n            PAnd \n            (P2 \n              PAnd \n              (P0 \n                (R2 RLe \n                    (R0 (E0 (EInt 5))) \n                    (R0 (E0 (ESym \"lo_discount\"))))) \n              (P0 \n                (R2 RLe \n                    (R0 (E0 (ESym \"lo_discount\"))) \n                    (R0 (E0 (EInt 7)))))) \n            (P0 \n              (R2 RLe \n                  (R0 (E0 (EInt 26))) \n                  (R0 (E0 (ESym \"lo_quantity\")))))) \n          (P0 \n            (R2 RLe (R0 (E0 (ESym \"lo_quantity\"))) (R0 (E0 (EInt 35))))))\n    ]))" << std::endl;
  {
    auto operation = mkSelect<CallableClass2>(Just<const std::string>("/tmp/fluidb_store/data32.dat"), Just<const std::string>("/tmp/fluidb_store/data33.dat"), "/tmp/fluidb_store/data28.dat");
    operation.run();
    operation.print_output(10);
  }
  // ForwardTrigger: (Just 
  //   (Right 
  //     [
  //      QGroup 
  //         [
  //          (
  //            ESym "revenue",
  //            E0 
  //               (NAggr 
  //                 AggrSum 
  //                 (E2 EMul 
  //                     (E0 (ESym "lo_extendedprice")) 
  //                     (E0 (ESym "lo_discount"))))
  //           )
  //         ] 
  //         []
  std::cout << "ForwardTrigger: (Just \n  (Right \n    [\n     QGroup \n        [\n         (\n           ESym \"revenue\",\n           E0 \n              (NAggr \n                AggrSum \n                (E2 EMul \n                    (E0 (ESym \"lo_extendedprice\")) \n                    (E0 (ESym \"lo_discount\"))))\n          )\n        ] \n        []\n    ]))" << std::endl;
  {
    auto operation = mkTotalAggregation<CallableClass7>(Just<const std::string>("/tmp/fluidb_store/data34.dat"), Just<const std::string>("/tmp/fluidb_store/data32.dat"), "/tmp/fluidb_store/data32.dat");
    operation.run();
    operation.print_output(10);
  }
  report_counters<60000>();
  return 0;
}
