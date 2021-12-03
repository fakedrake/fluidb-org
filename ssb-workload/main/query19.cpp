#include <array>

#include <string>

#include <codegen.hh>
class Record7 {
 public:
  Record7(unsigned __sortElem0, fluidb_string<10> __sortElem1) : sortElem0(__sortElem0), sortElem1(__sortElem1)
  {
  }
  Record7() 
  {
  }
  std::string show() const{
    std::stringstream o;
    o << sortElem0 << " | " << arrToString(sortElem1);
    return o.str();
  }
  bool operator <(const Record7& otherRec) const{
    return (otherRec.sortElem0 < sortElem0 && otherRec.sortElem1 < sortElem1);
  }
  bool operator ==(const Record7& otherRec) const{
    return (otherRec.sortElem0 == sortElem0 && otherRec.sortElem1 == sortElem1);
  }
  bool operator !=(const Record7& otherRec) const{
    return (otherRec.sortElem0 != sortElem0 || otherRec.sortElem1 != sortElem1);
  }
  unsigned sortElem0;
  fluidb_string<10> sortElem1;
 private:
};


class Record1 {
 public:
  Record1(unsigned __sym__0__5779451790266707509, unsigned __sym__0___45_7150856907437622963, fluidb_string<10> __sym__0___45_996409509835512161) : sym__0__5779451790266707509(__sym__0__5779451790266707509), sym__0___45_7150856907437622963(__sym__0___45_7150856907437622963), sym__0___45_996409509835512161(__sym__0___45_996409509835512161)
  {
  }
  Record1() 
  {
  }
  std::string show() const{
    std::stringstream o;
    o << sym__0__5779451790266707509 << " | " << sym__0___45_7150856907437622963 << " | " << arrToString(sym__0___45_996409509835512161);
    return o.str();
  }
  bool operator <(const Record1& otherRec) const{
    return (otherRec.sym__0__5779451790266707509 < sym__0__5779451790266707509 && (otherRec.sym__0___45_7150856907437622963 < sym__0___45_7150856907437622963 && otherRec.sym__0___45_996409509835512161 < sym__0___45_996409509835512161));
  }
  bool operator ==(const Record1& otherRec) const{
    return (otherRec.sym__0__5779451790266707509 == sym__0__5779451790266707509 && (otherRec.sym__0___45_7150856907437622963 == sym__0___45_7150856907437622963 && otherRec.sym__0___45_996409509835512161 == sym__0___45_996409509835512161));
  }
  bool operator !=(const Record1& otherRec) const{
    return (otherRec.sym__0__5779451790266707509 != sym__0__5779451790266707509 || (otherRec.sym__0___45_7150856907437622963 != sym__0___45_7150856907437622963 || otherRec.sym__0___45_996409509835512161 != sym__0___45_996409509835512161));
  }
  unsigned sym__0__5779451790266707509;
  unsigned sym__0___45_7150856907437622963;
  fluidb_string<10> sym__0___45_996409509835512161;
 private:
};


class Record3 {
 public:
  Record3(unsigned __sym__0___45_9062977367914427397, fluidb_string<25> __sym__0___45_2529420040722706896, fluidb_string<40> __sym__0__574837602909090142, fluidb_string<16> __sym__0__593663438043378012, fluidb_string<16> __sym__0___45_3887971984962307750, fluidb_string<13> __sym__0__5823252983849143071, fluidb_string<15> __sym__0___45_106394867614362960, unsigned __sym__0__6714030146677542844, int __sym__0__2887490991825418322, unsigned __sym__0__1712165930429801146, unsigned __sym__0___45_2770072266723735208, unsigned __sym__0___45_6459836926625833613, unsigned __sym__0__7430011083678934790, fluidb_string<21> __sym__0__8650139153845624051, unsigned __sym__0___45_2149426645220855235, fluidb_string<10> __sym__0___45_6782049901239695046, unsigned __sym__0__7592147203193979498, double __sym__0__4252747565219064764, unsigned __sym__0__7984101336509792358, double __sym__0___45_4696990900813234940, unsigned __sym__0___45_333127918406099392, unsigned __sym__0__1682335219184206071, double __sym__0___45_1968041223374194423, fluidb_string<13> __sym__0__7319886423946863313, unsigned __sym__0__5809761575032387040, fluidb_string<18> __sym__0__7089670065349420496, fluidb_string<9> __sym__0__6799464548088745306, fluidb_string<9> __sym__0__3432841002905483105, unsigned __sym__0__7494859993398015587, unsigned __sym__0___45_9206223202625852687, fluidb_string<7> __sym__0___45_7058002245744215800, unsigned __sym__0___45_6336476145267172161, unsigned __sym__0___45_394010879145796312, unsigned __sym__0__481431924301037312, unsigned __sym__0__5264647155959307922, unsigned __sym__0___45_6635260509808465407, fluidb_string<15> __sym__0__6067010566108174452, fluidb_string<2> __sym__0___45_4481069348252575620, fluidb_string<2> __sym__0__3235793487446917133, fluidb_string<2> __sym__0__4996330425147081765, fluidb_string<2> __sym__0___45_8340418653084059381, unsigned __sym__0__8129013918067253034, fluidb_string<55> __sym__0___45_7293426445565610246, fluidb_string<25> __sym__0__8820012445037342349, fluidb_string<7> __sym__0__6863506390321881387, fluidb_string<10> __sym__0___45_4545874988123414947, fluidb_string<11> __sym__0__1243206733405227801, fluidb_string<25> __sym__0__1972712326009675807, unsigned __sym__0__7671879331830055556, fluidb_string<10> __sym__0___45_6494031878969548547) : sym__0___45_9062977367914427397(__sym__0___45_9062977367914427397), sym__0___45_2529420040722706896(__sym__0___45_2529420040722706896), sym__0__574837602909090142(__sym__0__574837602909090142), sym__0__593663438043378012(__sym__0__593663438043378012), sym__0___45_3887971984962307750(__sym__0___45_3887971984962307750), sym__0__5823252983849143071(__sym__0__5823252983849143071), sym__0___45_106394867614362960(__sym__0___45_106394867614362960), sym__0__6714030146677542844(__sym__0__6714030146677542844), sym__0__2887490991825418322(__sym__0__2887490991825418322), sym__0__1712165930429801146(__sym__0__1712165930429801146), sym__0___45_2770072266723735208(__sym__0___45_2770072266723735208), sym__0___45_6459836926625833613(__sym__0___45_6459836926625833613), sym__0__7430011083678934790(__sym__0__7430011083678934790), sym__0__8650139153845624051(__sym__0__8650139153845624051), sym__0___45_2149426645220855235(__sym__0___45_2149426645220855235), sym__0___45_6782049901239695046(__sym__0___45_6782049901239695046), sym__0__7592147203193979498(__sym__0__7592147203193979498), sym__0__4252747565219064764(__sym__0__4252747565219064764), sym__0__7984101336509792358(__sym__0__7984101336509792358), sym__0___45_4696990900813234940(__sym__0___45_4696990900813234940), sym__0___45_333127918406099392(__sym__0___45_333127918406099392), sym__0__1682335219184206071(__sym__0__1682335219184206071), sym__0___45_1968041223374194423(__sym__0___45_1968041223374194423), sym__0__7319886423946863313(__sym__0__7319886423946863313), sym__0__5809761575032387040(__sym__0__5809761575032387040), sym__0__7089670065349420496(__sym__0__7089670065349420496), sym__0__6799464548088745306(__sym__0__6799464548088745306), sym__0__3432841002905483105(__sym__0__3432841002905483105), sym__0__7494859993398015587(__sym__0__7494859993398015587), sym__0___45_9206223202625852687(__sym__0___45_9206223202625852687), sym__0___45_7058002245744215800(__sym__0___45_7058002245744215800), sym__0___45_6336476145267172161(__sym__0___45_6336476145267172161), sym__0___45_394010879145796312(__sym__0___45_394010879145796312), sym__0__481431924301037312(__sym__0__481431924301037312), sym__0__5264647155959307922(__sym__0__5264647155959307922), sym__0___45_6635260509808465407(__sym__0___45_6635260509808465407), sym__0__6067010566108174452(__sym__0__6067010566108174452), sym__0___45_4481069348252575620(__sym__0___45_4481069348252575620), sym__0__3235793487446917133(__sym__0__3235793487446917133), sym__0__4996330425147081765(__sym__0__4996330425147081765), sym__0___45_8340418653084059381(__sym__0___45_8340418653084059381), sym__0__8129013918067253034(__sym__0__8129013918067253034), sym__0___45_7293426445565610246(__sym__0___45_7293426445565610246), sym__0__8820012445037342349(__sym__0__8820012445037342349), sym__0__6863506390321881387(__sym__0__6863506390321881387), sym__0___45_4545874988123414947(__sym__0___45_4545874988123414947), sym__0__1243206733405227801(__sym__0__1243206733405227801), sym__0__1972712326009675807(__sym__0__1972712326009675807), sym__0__7671879331830055556(__sym__0__7671879331830055556), sym__0___45_6494031878969548547(__sym__0___45_6494031878969548547)
  {
  }
  Record3() 
  {
  }
  std::string show() const{
    std::stringstream o;
    o << sym__0___45_9062977367914427397 << " | " << arrToString(sym__0___45_2529420040722706896) << " | " << arrToString(sym__0__574837602909090142) << " | " << arrToString(sym__0__593663438043378012) << " | " << arrToString(sym__0___45_3887971984962307750) << " | " << arrToString(sym__0__5823252983849143071) << " | " << arrToString(sym__0___45_106394867614362960) << " | " << sym__0__6714030146677542844 << " | " << sym__0__2887490991825418322 << " | " << sym__0__1712165930429801146 << " | " << sym__0___45_2770072266723735208 << " | " << sym__0___45_6459836926625833613 << " | " << sym__0__7430011083678934790 << " | " << arrToString(sym__0__8650139153845624051) << " | " << sym__0___45_2149426645220855235 << " | " << arrToString(sym__0___45_6782049901239695046) << " | " << sym__0__7592147203193979498 << " | " << sym__0__4252747565219064764 << " | " << sym__0__7984101336509792358 << " | " << sym__0___45_4696990900813234940 << " | " << sym__0___45_333127918406099392 << " | " << sym__0__1682335219184206071 << " | " << sym__0___45_1968041223374194423 << " | " << arrToString(sym__0__7319886423946863313) << " | " << sym__0__5809761575032387040 << " | " << arrToString(sym__0__7089670065349420496) << " | " << arrToString(sym__0__6799464548088745306) << " | " << arrToString(sym__0__3432841002905483105) << " | " << sym__0__7494859993398015587 << " | " << sym__0___45_9206223202625852687 << " | " << arrToString(sym__0___45_7058002245744215800) << " | " << sym__0___45_6336476145267172161 << " | " << sym__0___45_394010879145796312 << " | " << sym__0__481431924301037312 << " | " << sym__0__5264647155959307922 << " | " << sym__0___45_6635260509808465407 << " | " << arrToString(sym__0__6067010566108174452) << " | " << arrToString(sym__0___45_4481069348252575620) << " | " << arrToString(sym__0__3235793487446917133) << " | " << arrToString(sym__0__4996330425147081765) << " | " << arrToString(sym__0___45_8340418653084059381) << " | " << sym__0__8129013918067253034 << " | " << arrToString(sym__0___45_7293426445565610246) << " | " << arrToString(sym__0__8820012445037342349) << " | " << arrToString(sym__0__6863506390321881387) << " | " << arrToString(sym__0___45_4545874988123414947) << " | " << arrToString(sym__0__1243206733405227801) << " | " << arrToString(sym__0__1972712326009675807) << " | " << sym__0__7671879331830055556 << " | " << arrToString(sym__0___45_6494031878969548547);
    return o.str();
  }
  bool operator <(const Record3& otherRec) const{
    return (otherRec.sym__0___45_9062977367914427397 < sym__0___45_9062977367914427397 && (otherRec.sym__0___45_2529420040722706896 < sym__0___45_2529420040722706896 && (otherRec.sym__0__574837602909090142 < sym__0__574837602909090142 && (otherRec.sym__0__593663438043378012 < sym__0__593663438043378012 && (otherRec.sym__0___45_3887971984962307750 < sym__0___45_3887971984962307750 && (otherRec.sym__0__5823252983849143071 < sym__0__5823252983849143071 && (otherRec.sym__0___45_106394867614362960 < sym__0___45_106394867614362960 && (otherRec.sym__0__6714030146677542844 < sym__0__6714030146677542844 && (otherRec.sym__0__2887490991825418322 < sym__0__2887490991825418322 && (otherRec.sym__0__1712165930429801146 < sym__0__1712165930429801146 && (otherRec.sym__0___45_2770072266723735208 < sym__0___45_2770072266723735208 && (otherRec.sym__0___45_6459836926625833613 < sym__0___45_6459836926625833613 && (otherRec.sym__0__7430011083678934790 < sym__0__7430011083678934790 && (otherRec.sym__0__8650139153845624051 < sym__0__8650139153845624051 && (otherRec.sym__0___45_2149426645220855235 < sym__0___45_2149426645220855235 && (otherRec.sym__0___45_6782049901239695046 < sym__0___45_6782049901239695046 && (otherRec.sym__0__7592147203193979498 < sym__0__7592147203193979498 && (otherRec.sym__0__4252747565219064764 < sym__0__4252747565219064764 && (otherRec.sym__0__7984101336509792358 < sym__0__7984101336509792358 && (otherRec.sym__0___45_4696990900813234940 < sym__0___45_4696990900813234940 && (otherRec.sym__0___45_333127918406099392 < sym__0___45_333127918406099392 && (otherRec.sym__0__1682335219184206071 < sym__0__1682335219184206071 && (otherRec.sym__0___45_1968041223374194423 < sym__0___45_1968041223374194423 && (otherRec.sym__0__7319886423946863313 < sym__0__7319886423946863313 && (otherRec.sym__0__5809761575032387040 < sym__0__5809761575032387040 && (otherRec.sym__0__7089670065349420496 < sym__0__7089670065349420496 && (otherRec.sym__0__6799464548088745306 < sym__0__6799464548088745306 && (otherRec.sym__0__3432841002905483105 < sym__0__3432841002905483105 && (otherRec.sym__0__7494859993398015587 < sym__0__7494859993398015587 && (otherRec.sym__0___45_9206223202625852687 < sym__0___45_9206223202625852687 && (otherRec.sym__0___45_7058002245744215800 < sym__0___45_7058002245744215800 && (otherRec.sym__0___45_6336476145267172161 < sym__0___45_6336476145267172161 && (otherRec.sym__0___45_394010879145796312 < sym__0___45_394010879145796312 && (otherRec.sym__0__481431924301037312 < sym__0__481431924301037312 && (otherRec.sym__0__5264647155959307922 < sym__0__5264647155959307922 && (otherRec.sym__0___45_6635260509808465407 < sym__0___45_6635260509808465407 && (otherRec.sym__0__6067010566108174452 < sym__0__6067010566108174452 && (otherRec.sym__0___45_4481069348252575620 < sym__0___45_4481069348252575620 && (otherRec.sym__0__3235793487446917133 < sym__0__3235793487446917133 && (otherRec.sym__0__4996330425147081765 < sym__0__4996330425147081765 && (otherRec.sym__0___45_8340418653084059381 < sym__0___45_8340418653084059381 && (otherRec.sym__0__8129013918067253034 < sym__0__8129013918067253034 && (otherRec.sym__0___45_7293426445565610246 < sym__0___45_7293426445565610246 && (otherRec.sym__0__8820012445037342349 < sym__0__8820012445037342349 && (otherRec.sym__0__6863506390321881387 < sym__0__6863506390321881387 && (otherRec.sym__0___45_4545874988123414947 < sym__0___45_4545874988123414947 && (otherRec.sym__0__1243206733405227801 < sym__0__1243206733405227801 && (otherRec.sym__0__1972712326009675807 < sym__0__1972712326009675807 && (otherRec.sym__0__7671879331830055556 < sym__0__7671879331830055556 && otherRec.sym__0___45_6494031878969548547 < sym__0___45_6494031878969548547)))))))))))))))))))))))))))))))))))))))))))))))));
  }
  bool operator ==(const Record3& otherRec) const{
    return (otherRec.sym__0___45_9062977367914427397 == sym__0___45_9062977367914427397 && (otherRec.sym__0___45_2529420040722706896 == sym__0___45_2529420040722706896 && (otherRec.sym__0__574837602909090142 == sym__0__574837602909090142 && (otherRec.sym__0__593663438043378012 == sym__0__593663438043378012 && (otherRec.sym__0___45_3887971984962307750 == sym__0___45_3887971984962307750 && (otherRec.sym__0__5823252983849143071 == sym__0__5823252983849143071 && (otherRec.sym__0___45_106394867614362960 == sym__0___45_106394867614362960 && (otherRec.sym__0__6714030146677542844 == sym__0__6714030146677542844 && (otherRec.sym__0__2887490991825418322 == sym__0__2887490991825418322 && (otherRec.sym__0__1712165930429801146 == sym__0__1712165930429801146 && (otherRec.sym__0___45_2770072266723735208 == sym__0___45_2770072266723735208 && (otherRec.sym__0___45_6459836926625833613 == sym__0___45_6459836926625833613 && (otherRec.sym__0__7430011083678934790 == sym__0__7430011083678934790 && (otherRec.sym__0__8650139153845624051 == sym__0__8650139153845624051 && (otherRec.sym__0___45_2149426645220855235 == sym__0___45_2149426645220855235 && (otherRec.sym__0___45_6782049901239695046 == sym__0___45_6782049901239695046 && (otherRec.sym__0__7592147203193979498 == sym__0__7592147203193979498 && (otherRec.sym__0__4252747565219064764 == sym__0__4252747565219064764 && (otherRec.sym__0__7984101336509792358 == sym__0__7984101336509792358 && (otherRec.sym__0___45_4696990900813234940 == sym__0___45_4696990900813234940 && (otherRec.sym__0___45_333127918406099392 == sym__0___45_333127918406099392 && (otherRec.sym__0__1682335219184206071 == sym__0__1682335219184206071 && (otherRec.sym__0___45_1968041223374194423 == sym__0___45_1968041223374194423 && (otherRec.sym__0__7319886423946863313 == sym__0__7319886423946863313 && (otherRec.sym__0__5809761575032387040 == sym__0__5809761575032387040 && (otherRec.sym__0__7089670065349420496 == sym__0__7089670065349420496 && (otherRec.sym__0__6799464548088745306 == sym__0__6799464548088745306 && (otherRec.sym__0__3432841002905483105 == sym__0__3432841002905483105 && (otherRec.sym__0__7494859993398015587 == sym__0__7494859993398015587 && (otherRec.sym__0___45_9206223202625852687 == sym__0___45_9206223202625852687 && (otherRec.sym__0___45_7058002245744215800 == sym__0___45_7058002245744215800 && (otherRec.sym__0___45_6336476145267172161 == sym__0___45_6336476145267172161 && (otherRec.sym__0___45_394010879145796312 == sym__0___45_394010879145796312 && (otherRec.sym__0__481431924301037312 == sym__0__481431924301037312 && (otherRec.sym__0__5264647155959307922 == sym__0__5264647155959307922 && (otherRec.sym__0___45_6635260509808465407 == sym__0___45_6635260509808465407 && (otherRec.sym__0__6067010566108174452 == sym__0__6067010566108174452 && (otherRec.sym__0___45_4481069348252575620 == sym__0___45_4481069348252575620 && (otherRec.sym__0__3235793487446917133 == sym__0__3235793487446917133 && (otherRec.sym__0__4996330425147081765 == sym__0__4996330425147081765 && (otherRec.sym__0___45_8340418653084059381 == sym__0___45_8340418653084059381 && (otherRec.sym__0__8129013918067253034 == sym__0__8129013918067253034 && (otherRec.sym__0___45_7293426445565610246 == sym__0___45_7293426445565610246 && (otherRec.sym__0__8820012445037342349 == sym__0__8820012445037342349 && (otherRec.sym__0__6863506390321881387 == sym__0__6863506390321881387 && (otherRec.sym__0___45_4545874988123414947 == sym__0___45_4545874988123414947 && (otherRec.sym__0__1243206733405227801 == sym__0__1243206733405227801 && (otherRec.sym__0__1972712326009675807 == sym__0__1972712326009675807 && (otherRec.sym__0__7671879331830055556 == sym__0__7671879331830055556 && otherRec.sym__0___45_6494031878969548547 == sym__0___45_6494031878969548547)))))))))))))))))))))))))))))))))))))))))))))))));
  }
  bool operator !=(const Record3& otherRec) const{
    return (otherRec.sym__0___45_9062977367914427397 != sym__0___45_9062977367914427397 || (otherRec.sym__0___45_2529420040722706896 != sym__0___45_2529420040722706896 || (otherRec.sym__0__574837602909090142 != sym__0__574837602909090142 || (otherRec.sym__0__593663438043378012 != sym__0__593663438043378012 || (otherRec.sym__0___45_3887971984962307750 != sym__0___45_3887971984962307750 || (otherRec.sym__0__5823252983849143071 != sym__0__5823252983849143071 || (otherRec.sym__0___45_106394867614362960 != sym__0___45_106394867614362960 || (otherRec.sym__0__6714030146677542844 != sym__0__6714030146677542844 || (otherRec.sym__0__2887490991825418322 != sym__0__2887490991825418322 || (otherRec.sym__0__1712165930429801146 != sym__0__1712165930429801146 || (otherRec.sym__0___45_2770072266723735208 != sym__0___45_2770072266723735208 || (otherRec.sym__0___45_6459836926625833613 != sym__0___45_6459836926625833613 || (otherRec.sym__0__7430011083678934790 != sym__0__7430011083678934790 || (otherRec.sym__0__8650139153845624051 != sym__0__8650139153845624051 || (otherRec.sym__0___45_2149426645220855235 != sym__0___45_2149426645220855235 || (otherRec.sym__0___45_6782049901239695046 != sym__0___45_6782049901239695046 || (otherRec.sym__0__7592147203193979498 != sym__0__7592147203193979498 || (otherRec.sym__0__4252747565219064764 != sym__0__4252747565219064764 || (otherRec.sym__0__7984101336509792358 != sym__0__7984101336509792358 || (otherRec.sym__0___45_4696990900813234940 != sym__0___45_4696990900813234940 || (otherRec.sym__0___45_333127918406099392 != sym__0___45_333127918406099392 || (otherRec.sym__0__1682335219184206071 != sym__0__1682335219184206071 || (otherRec.sym__0___45_1968041223374194423 != sym__0___45_1968041223374194423 || (otherRec.sym__0__7319886423946863313 != sym__0__7319886423946863313 || (otherRec.sym__0__5809761575032387040 != sym__0__5809761575032387040 || (otherRec.sym__0__7089670065349420496 != sym__0__7089670065349420496 || (otherRec.sym__0__6799464548088745306 != sym__0__6799464548088745306 || (otherRec.sym__0__3432841002905483105 != sym__0__3432841002905483105 || (otherRec.sym__0__7494859993398015587 != sym__0__7494859993398015587 || (otherRec.sym__0___45_9206223202625852687 != sym__0___45_9206223202625852687 || (otherRec.sym__0___45_7058002245744215800 != sym__0___45_7058002245744215800 || (otherRec.sym__0___45_6336476145267172161 != sym__0___45_6336476145267172161 || (otherRec.sym__0___45_394010879145796312 != sym__0___45_394010879145796312 || (otherRec.sym__0__481431924301037312 != sym__0__481431924301037312 || (otherRec.sym__0__5264647155959307922 != sym__0__5264647155959307922 || (otherRec.sym__0___45_6635260509808465407 != sym__0___45_6635260509808465407 || (otherRec.sym__0__6067010566108174452 != sym__0__6067010566108174452 || (otherRec.sym__0___45_4481069348252575620 != sym__0___45_4481069348252575620 || (otherRec.sym__0__3235793487446917133 != sym__0__3235793487446917133 || (otherRec.sym__0__4996330425147081765 != sym__0__4996330425147081765 || (otherRec.sym__0___45_8340418653084059381 != sym__0___45_8340418653084059381 || (otherRec.sym__0__8129013918067253034 != sym__0__8129013918067253034 || (otherRec.sym__0___45_7293426445565610246 != sym__0___45_7293426445565610246 || (otherRec.sym__0__8820012445037342349 != sym__0__8820012445037342349 || (otherRec.sym__0__6863506390321881387 != sym__0__6863506390321881387 || (otherRec.sym__0___45_4545874988123414947 != sym__0___45_4545874988123414947 || (otherRec.sym__0__1243206733405227801 != sym__0__1243206733405227801 || (otherRec.sym__0__1972712326009675807 != sym__0__1972712326009675807 || (otherRec.sym__0__7671879331830055556 != sym__0__7671879331830055556 || otherRec.sym__0___45_6494031878969548547 != sym__0___45_6494031878969548547)))))))))))))))))))))))))))))))))))))))))))))))));
  }
  unsigned sym__0___45_9062977367914427397;
  fluidb_string<25> sym__0___45_2529420040722706896;
  fluidb_string<40> sym__0__574837602909090142;
  fluidb_string<16> sym__0__593663438043378012;
  fluidb_string<16> sym__0___45_3887971984962307750;
  fluidb_string<13> sym__0__5823252983849143071;
  fluidb_string<15> sym__0___45_106394867614362960;
  unsigned sym__0__6714030146677542844;
  int sym__0__2887490991825418322;
  unsigned sym__0__1712165930429801146;
  unsigned sym__0___45_2770072266723735208;
  unsigned sym__0___45_6459836926625833613;
  unsigned sym__0__7430011083678934790;
  fluidb_string<21> sym__0__8650139153845624051;
  unsigned sym__0___45_2149426645220855235;
  fluidb_string<10> sym__0___45_6782049901239695046;
  unsigned sym__0__7592147203193979498;
  double sym__0__4252747565219064764;
  unsigned sym__0__7984101336509792358;
  double sym__0___45_4696990900813234940;
  unsigned sym__0___45_333127918406099392;
  unsigned sym__0__1682335219184206071;
  double sym__0___45_1968041223374194423;
  fluidb_string<13> sym__0__7319886423946863313;
  unsigned sym__0__5809761575032387040;
  fluidb_string<18> sym__0__7089670065349420496;
  fluidb_string<9> sym__0__6799464548088745306;
  fluidb_string<9> sym__0__3432841002905483105;
  unsigned sym__0__7494859993398015587;
  unsigned sym__0___45_9206223202625852687;
  fluidb_string<7> sym__0___45_7058002245744215800;
  unsigned sym__0___45_6336476145267172161;
  unsigned sym__0___45_394010879145796312;
  unsigned sym__0__481431924301037312;
  unsigned sym__0__5264647155959307922;
  unsigned sym__0___45_6635260509808465407;
  fluidb_string<15> sym__0__6067010566108174452;
  fluidb_string<2> sym__0___45_4481069348252575620;
  fluidb_string<2> sym__0__3235793487446917133;
  fluidb_string<2> sym__0__4996330425147081765;
  fluidb_string<2> sym__0___45_8340418653084059381;
  unsigned sym__0__8129013918067253034;
  fluidb_string<55> sym__0___45_7293426445565610246;
  fluidb_string<25> sym__0__8820012445037342349;
  fluidb_string<7> sym__0__6863506390321881387;
  fluidb_string<10> sym__0___45_4545874988123414947;
  fluidb_string<11> sym__0__1243206733405227801;
  fluidb_string<25> sym__0__1972712326009675807;
  unsigned sym__0__7671879331830055556;
  fluidb_string<10> sym__0___45_6494031878969548547;
 private:
};


class CallableClass8 {
 public:
  Record7 operator()(const Record3& record0) {
    return Record7(record0.sym__0__7494859993398015587, record0.sym__0___45_4545874988123414947);
  }
  typedef Record7 Codomain;
  typedef Record3 Domain0;
 private:
};


class CallableClass10 {
 public:
  Record7 operator()(const Record1& record9) {
    return Record7(record9.sym__0___45_7150856907437622963, record9.sym__0___45_996409509835512161);
  }
  typedef Record7 Codomain;
  typedef Record1 Domain0;
 private:
};


class CallableClass6 {
 public:
  Record1 operator()(const Record3& record0) {
    return Record1(vAggrSum2(record0.sym__0___45_333127918406099392), vAggrFirst4(record0.sym__0__7494859993398015587), vAggrFirst5(record0.sym__0___45_4545874988123414947));
  }
  typedef Record1 Codomain;
  typedef Record3 Domain0;
 private:
  AggrSum<unsigned> vAggrSum2;
  AggrFirst<unsigned> vAggrFirst4;
  AggrFirst<fluidb_string<10>> vAggrFirst5;
};






int main() {
  // ForwardTrigger: (Just 
  //   (Right 
  //     [
  //      QGroup 
  //         [(ESym "tmpSym0",E0 (NAggr AggrSum (E0 (ESym "lo_revenue")))),
  //          (ESym "d_year",E0 (NAggr AggrFirst (E0 (ESym "d_year")))),
  //          (ESym "p_brand1",E0 (NAggr AggrFirst (E0 (ESym "p_brand1"))))] 
  //         [E0 (ESym "d_year"),E0 (ESym "p_brand1")]
  std::cout << "ForwardTrigger: (Just \n  (Right \n    [\n     QGroup \n        [(ESym \"tmpSym0\",E0 (NAggr AggrSum (E0 (ESym \"lo_revenue\")))),\n         (ESym \"d_year\",E0 (NAggr AggrFirst (E0 (ESym \"d_year\")))),\n         (ESym \"p_brand1\",E0 (NAggr AggrFirst (E0 (ESym \"p_brand1\"))))] \n        [E0 (ESym \"d_year\"),E0 (ESym \"p_brand1\")]\n    ]))" << std::endl;
  {
    auto operation = mkAggregation<CallableClass6, CallableClass8>(Just<const std::string>("/tmp/fluidb_store/data146.dat"), Just<const std::string>("/tmp/fluidb_store/data120.dat"), "/tmp/fluidb_store/data120.dat");
    operation.run();
    operation.print_output(10);
  }
  std::cout << "ForwardTrigger: (Just (Right [QSort [E0 (ESym \"d_year\"),E0 (ESym \"p_brand1\")]]))" << std::endl;
  {
    auto operation = mkSort<CallableClass10>(Just<const std::string>("/tmp/fluidb_store/data147.dat"), Just<const std::string>("/tmp/fluidb_store/data146.dat"), "/tmp/fluidb_store/data146.dat");
    operation.run();
    operation.print_output(10);
  }
  report_counters<60000>();
  return 0;
}
