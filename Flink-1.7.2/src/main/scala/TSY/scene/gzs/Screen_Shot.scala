package TSY.scene.gzs

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * \* Created with IntelliJ IDEA.
  * \* User: aresyhzhang
  * \* Date: 2020/11/3
  * \* Time: 20:19
  * \*/
object Screen_Shot {
  def main(args: Array[String]): Unit = {

    def isExistsArr(elem:String,result_arr:Array[String]): Boolean ={
      var result=false
      // not bbb
      //此处涉及到用户取反操作
      if(elem.contains("not")){
        //如果结果集里包含这个元素，结果设为false，否则设置为true
        val isContain: Boolean = result_arr.contains(elem.substring(4,elem.length))
        if(isContain)result=false else result=true
      }
      //bbb
      else
      {
        if(result_arr.contains(elem))result=true else result=false
      }

      result
    }

    //进行与操作的判断，判断aaa and bbb是否同时存在于数组result_arr中
    //调用此函数意味着字符串中有and分隔符 比如 aaa and bbb
    def andIsExistArr(elem:String,result_arr:Array[String]): Boolean ={
      val tmp_arr: Array[String] = elem.split("and")

      //ccc,ddd
      val bool_arr:ArrayBuffer[Boolean]= ArrayBuffer[Boolean]()


      for (elem <- tmp_arr) {
        //判断元素是否在模型数组里
        //如果是true或者false代表是括号内已经进行过计算
        //如果elem是true或false，代表是括号内已经进行过计算
        if(elem=="true"|| elem=="false" || elem=="not true" || elem=="not false"){
          elem match{
            case "true"=>bool_arr.append(true)
            case "false"=>bool_arr.append(false)
            case "not true"=>bool_arr.append(false)
            case "not false"=>bool_arr.append(true)
          }
        }
        else{
        bool_arr.append(isExistsArr(elem,result_arr))
        }
      }

      //如果数组里包含false，由于是&操作，需要返回false，否则不包含false返回true
      if(bool_arr.contains(false)) false else  true
    }


    def mainIsExits(input:String,result_arr: Array[String]): Boolean ={
      var tmpBool=false
      var policy_arr: Array[String] = Array[String]()

      //如果用户字符串中包含or example: aaa or !bbb or ccc&ddd or true or false
      if(input.contains("or")){
        policy_arr = input.split("or")
        val or_bool_arr:ArrayBuffer[Boolean]= ArrayBuffer[Boolean]()

        //aaa,!bbb,ccc&ddd,true,false
        for (elem <- policy_arr) {

          //如果elem是true或false，代表是括号内已经进行过计算
          if(elem=="true"|| elem=="false" || elem=="not true" || elem=="not false"){
            elem match{
              case "true"=>or_bool_arr.append(true)
              case "false"=>or_bool_arr.append(false)
              case "not true"=>or_bool_arr.append(false)
              case "not false"=>or_bool_arr.append(true)
            }
          }

          //如果包含and操作符,ccc and ddd
          else if(elem.contains("and")){

            if(andIsExistArr(elem,result_arr))or_bool_arr.append(true) else or_bool_arr.append(false)

          }
            //如果该字段既不是true或false也不包含and，说明只是单纯的值 aaa
          else{
            //判断元素是否在模型数组里
            if(isExistsArr(elem,result_arr))or_bool_arr.append(true) else or_bool_arr.append(false)
          }

        }
        if(or_bool_arr.contains(true))tmpBool=true else tmpBool=false

      }

      //如果表达式没包含or，只有&
      else if(input.contains("and")) {

        if(andIsExistArr(input,result_arr))tmpBool=true else tmpBool=false

      }


      //如果既没有&也没有or
      else{


        tmpBool=isExistsArr(input,result_arr)

      }
      tmpBool

    }

    /**
      会传入的几种情况
     * aaa
     * !aaa
     * aaa&bb
     * aaaorbbb
     * (aaa&bbb)orcccor(eee&fff)
     * (aaaorbbb)&ccc&(dddoreee)
     * ((aaaorbbb)&ccc)&(dddoreee)
      */

    val model_result: String = "aaa,ccc,bbb"
    //zhunxin_white and not (renwu_ren or waigua_info)

    val model_arr: Array[String] = model_result.split(',')
    var flag=false

//    var str = "(aaa&bbb)or(ccc&ddd)"
//    var str = "(aaaorbbb)&(cccorddd)"
//    var str = "aaa&bbb"
//    var str = "aaaorggg"
    var str = "bbb"
//    var str = "!ggg"

    //如果用户在字符串中有使用（）提升优先级
    if (str.contains('(') && str.contains(')')){
      val pattern = new Regex("(?<=\\()[^\\)]+")

      //取出括号内需要计算的逻辑
      val regex_arr: Array[String] = pattern.findAllIn(str).toArray

      //aorb,eorf

      for (elem <- regex_arr) {
        //把每次括号内计算的结果都保存下来
        val result: Boolean = mainIsExits(elem,model_arr)
        //替换原字符串中（）内的值为计算的结果true或者false
        //(aorb)&(eorf)&gorh -> (true)&(false)&gorh
        str=str.replace("("+elem+")",result.toString)
      }

    }
    //zhunxin_white and not true

    flag=mainIsExits(str,model_arr)

    println(flag)









  }
}