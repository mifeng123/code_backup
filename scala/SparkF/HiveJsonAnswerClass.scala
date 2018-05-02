package SparkF

import org.json.{JSONArray, JSONObject}

import scala.util.control.Breaks
//import scala.util.matching.Regex

/**
  * Created by ztf on 2017/8/14.
  */
class HiveJsonAnswerClass{
//String con ,int subject_id ,int type_id

   def  structOne(con: String, subject_id: Int,type_id: Int): String = {
      var body_text=0 //题干文字数
      var body_img=0 //题干图片数
      var option_text=0 //选择文字数
      var option_img=0 //选项图片数
      var answer_text=0 //答案文字数
      var answer_img=0 //答案图片数
      var analysis_text=0 //解析文字数
      var analysis_img=0 //解析图片数
      var text_answer_value="" //答案内容
      var text_analysis_value = "" //解析内容
     var contentJsonObject =new JSONObject()
     try{
       contentJsonObject = new JSONObject(new JSONObject(con).get("content").toString)
     }catch{
       case e:Exception => {
         contentJsonObject = new JSONObject(con.toString)
       }
     }
   //body部分
      if(type_id!=14){
      var bodyJsonArray = contentJsonObject.getJSONArray("body")
      for(i <- 0 to (bodyJsonArray.length()-1)) {
         //println(i+"开始计数")
         try {
         var bodyJsonObject = new JSONObject(bodyJsonArray.get(i).toString)
         var bodyType = bodyJsonObject.get("type").toString.trim
         var bodyValue = bodyJsonObject.get("value").toString.trim
         if ("formula".equals(bodyType) || "image".equals(bodyType)) {
            body_img = body_img + 1
         }
         if ("text".equals(bodyType)) {
            body_text = body_text + bodyValue.length
         }
      }catch {
            case e: Exception => {if ("Exception".equals(e)) {
             new Breaks().break()
         }}
         }/*finally {
            new Breaks().break()
         }*/
      }
   }
   //answer部分
       var answerJsonArray =  contentJsonObject.getJSONArray("answer")
      var text_answer_value_temp =""
      for( i <- 0 to answerJsonArray.length()-1){
           try{
               var answerValue = answerJsonArray.get(i).toString.trim
              if(!answerValue.contains("http:")){
                 answer_text=answer_text + answerValue.length
                 text_answer_value_temp=text_answer_value_temp+answerValue
              }
              if(answerValue.contains("http:")){
                 answer_img=answer_img+1
              }
               text_answer_value_temp = text_answer_value_temp.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
               text_answer_value = text_answer_value_temp
              if("n".equals(text_answer_value) || "答案略".equals(text_answer_value) || "省略".equals(text_answer_value) || "略".equals(text_answer_value) || "无".equals(text_answer_value) || "暂无".equals(text_answer_value) || text_answer_value==null){
                text_answer_value = "答案为空"
                 answer_text = 0
                 answer_img = 0
              }
           }  catch {
              case e: Exception => {if ("Exception".equals(e)) {
                 println("有异常")
                 new Breaks().break()
              }}
           }
      }
    //options部分
      var optionsJsonArray = contentJsonObject.getJSONArray("options")
      if(optionsJsonArray.length() == 4 || (subject_id == 5 || subject_id == 6) && optionsJsonArray.length() == 3){
      for(i <- 0 to optionsJsonArray.length()-1){
         try{
            var optionJsonArray = optionsJsonArray.getJSONArray(i)
            var optainJsonObject = optionJsonArray.getJSONObject(1)
            var optionValue = optainJsonObject.get("value").toString
                 optionValue = optionValue.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
                  if(optionValue != null && !"省略".equals(optionValue) && !"无".equals(optionValue)){
                    for( k <- 0 to 1) {
                       try {
                          var optionsJsonObject = optionJsonArray.getJSONObject(k)
                          var optionsType = optionsJsonObject.get("type").toString
                          var optionsValue = optionsJsonObject.get("value").toString
                          if ("formula".equals(optionsType) || "image".equals(optionsType)) {
                             option_img = option_img + 1

                          }
                          if ("option".equals(optionsType) || "text".equals(optionsType)) {
                             option_text = option_text + optionsValue.length
                          }
                       } catch {
                          case e: Exception => {
                             if ("Exception".equals(e)) {
                                println("有异常")
                                new Breaks().break()
                             }
                          }
                       }
                    }
                  }else{
                     //
                     option_img = 0
                     option_text = 0
                     body_img = 0
                     body_text =0
                     new Breaks().break()
                  }
         }catch {
            case e: Exception => {if ("Exception".equals(e)) {
               println("有异常")
               new Breaks().break()
            }}
         }
      }
      }else {
         //
         option_img = 0
         option_text = 0
         body_img = 0
         body_text =0
      }
    //analysis部分
      var analysisJsonArray = new JSONArray()
      var  text_analysis_value_temp =""
      try{
         analysisJsonArray=contentJsonObject.getJSONArray("analysis")
      }catch {
         case e:Exception => {
           println("开始")
           var analysisValue = contentJsonObject.get("analysis").toString.trim
           if(!analysisValue.contains("http:")){
             analysis_text = analysis_text+analysisValue.length
             text_analysis_value_temp = text_analysis_value_temp + analysisValue
           }
           if(analysisValue.contains("http:")){
             analysis_img = analysis_img + 1
           }
           println("结束")
         }


      }
      for(i <- 0 to analysisJsonArray.length()-1){
         try{
            var analysisJsonObject = analysisJsonArray.getJSONObject(i)
            var analysisValue = analysisJsonObject.get("value").toString.trim
            var analysisType = analysisJsonObject.get("type").toString.trim
            if("text".equals(analysisType)){
               analysis_text = analysis_text+analysisValue.length
               text_analysis_value_temp = text_analysis_value_temp + analysisValue
            }
            if("formula".equals(analysisType) || "image".equals(analysisType)){
               analysis_img = analysis_img+1
            }
         }catch {
            case e: Exception => {if ("Exception".equals(e)) {
               println("有异常")
               new Breaks().break()
            }}
         }
      //
         text_analysis_value_temp = text_analysis_value_temp.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
         text_analysis_value=text_analysis_value_temp
         if("解析略".equals(text_analysis_value) || "省略".equals(text_analysis_value) || "略".equals(text_analysis_value) || "无".equals(text_analysis_value) || "暂无".equals(text_analysis_value) || "n".equals(text_analysis_value) || text_analysis_value==null){
            text_analysis_value_temp = "解析为空"
            text_analysis_value = text_analysis_value_temp
            analysis_img = 0
            analysis_text = 0
         }
      }
     body_text +"|"+ body_img + "|" + answer_text + "|" + answer_img + "|" + analysis_text + "|"+ analysis_img + "|" + text_analysis_value + "|" + text_answer_value
     // body_text + "," + body_img + "," + answer_text + "," + answer_img + "," + analysis_text + ","+ analysis_img + "," + text_analysis_value + "," + text_answer_value

   }
   def  structNotOne(con: String, bool: Boolean,type_id: Int): String = {
     var body_text=0 //题干文字数
     var body_img=0 //题干图片数
     var option_text=0 //选择文字数
     var option_img=0 //选项图片数
     var answer_text=0 //答案文字数
     var answer_img=0 //答案图片数
     var analysis_text=0 //解析文字数
     var analysis_img=0 //解析图片数
     var text_answer_value="" //答案内容
     var text_analysis_value = "" //解析内容
     var contentValue = new JSONObject(con).get("content")
     var contentJsonObject = new JSONObject(contentValue.toString)
     var answerValueSum = ""
     //body 部分
     var  bodyJsonArray = contentJsonObject.getJSONArray("body")
     for(i <- 0 to bodyJsonArray.length()-1 ){
       try{
         var bodyJsonObject = bodyJsonArray.getJSONObject(i)
         var bodyType = bodyJsonObject.get("type").toString
         var bodyValue = bodyJsonObject.get("value").toString
         if("formula".equals(bodyType) || "image".equals(bodyType)){
           body_img = body_img+ + 1
         }
         if("text".equals(bodyType)){
           body_text = body_text + bodyValue.length
         }
       }catch{
         case e: Exception => {if ("Exception".equals(e)) {
           println("有异常")
           new Breaks().break()
         }}
       }

     }
     //answer部分
     var text_answer_value_temp =""
     if(bool){
       if(type_id == 2){
         var groupJsonArray = new JSONArray()
         var bool_group = true
         var len = contentJsonObject.getJSONArray("answer").length()
         var answerJsonObject = new JSONObject()
         if(len <= 1){
           // answerJsonObject = contentJsonObject.getJSONObject("answer")
           answerJsonObject = new JSONObject(contentJsonObject.getJSONArray("answer").get(0).toString)
         }else{
           groupJsonArray = contentJsonObject.getJSONArray("answer")
           bool_group = false
         }
         try{
           if(bool_group){
             /*print(answerJsonObject.get("group").toString+"我看看是什么1鬼")
             print(answerJsonObject.getJSONArray("group").length()+"我看看是什么2鬼")
             groupJsonArray = new JSONArray(answerJsonObject.get("group").toString)
             println(groupJsonArray.get(3)+"这是啥鬼")*/
            groupJsonArray = answerJsonObject.getJSONArray("group")
           }
         }catch {
           case e: Exception => {
             new Breaks().break()
           }
         }
         print("为啥不进来1")
         for(i <- 0 to groupJsonArray.length()-1){
           print("终于进来了2")
           try{
             var groupJsonObject = groupJsonArray.getJSONObject(i)

             var groupType =""
             var groupValue = ""
             if(groupJsonObject.toString().contains("group")){
               var group_array = groupJsonObject.getJSONArray("group")
               var group_object =new JSONObject(group_array.get(0).toString)
               groupType= group_object.get("type").toString.trim
               groupValue=group_object.get("value").toString.trim
             }else{
               groupType = groupJsonObject.get("type").toString.trim
               groupValue = groupJsonObject.get("value").toString.trim
             }

             if("formula".equals(groupType) || "image".equals(groupType)){
               answer_img = answer_img + 1
             }
             if("text".equals(groupType)){
               answer_text = answer_text  + groupValue.length
               text_answer_value_temp = text_answer_value_temp + groupValue
             }
           }catch{
             case e: Exception => {if ("Exception".equals(e)) {
               println("有异常")
               new Breaks().break()
             }}
           }

         }
         println(text_answer_value_temp+"这是答案")
       }else if (type_id == 3){
         var answerValue = contentJsonObject.get("answer").toString
         if(!answerValue.contains("http:")){
           answer_text= answer_text + answerValue.length
           text_answer_value_temp=text_answer_value_temp+ answerValue
         }
         if(answerValue.contains("http:")){
           answer_img = answer_img + 1
         }

       }else{
         var answerJsonArray = new JSONArray()
         try{
           answerJsonArray = contentJsonObject.getJSONArray("answer")
         }catch{
           case e:Exception => {
              var answerValue = contentJsonObject.get("answer").toString.trim
             answerValue = answerValue.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
             if(!answerValue.contains("http:")){
               if("答案见解析".equals(answerValue)){
                 answer_text = answer_text + answerValue.length
                 text_answer_value_temp = text_answer_value_temp + answerValue
               }
             }
             if(answerValue.contains("http:")){
               answer_img = answer_img + 1
             }
           }
         }
         print("为啥不进来")
         for(i <- 0 to answerJsonArray.length()-1){
           print("终于进来了")
           try{
             var answerValues = answerJsonArray.get(i).toString
             var answerJsonObject = new JSONObject(answerValues)
             var answerType =""
             var answerValue = ""
             if(answerValues.contains("group")){
               var groupJsonArray = answerJsonObject.getJSONArray("group")
               var groupJsonObject =  groupJsonArray.getJSONObject(i)
               answerValue = groupJsonObject.get("value").toString
               answerType = groupJsonObject.get("type").toString
             }else{
               answerValue = answerJsonObject.get("value").toString
               answerType = answerJsonObject.get("type").toString
             }
             //
             if("text".equals(answerType)){
               answer_text = answer_text +  answerValue.length
               answerValueSum = answerValueSum + answerValue
               text_answer_value_temp = text_answer_value_temp + answerValue
             }
             if("formula".equals(answerType) || "image".equals(answerType)){
               answer_img = answer_img + 1
             }
           }catch{
             case e: Exception => {if ("Exception".equals(e)) {
               println("有异常")
               new Breaks().break()
             }}
           }

         }
       }
     }
     if(!bool){
       var typeJson = contentJsonObject.get("type").toString
       var typeJsonObject = new JSONObject(typeJson)
       var typeValue = typeJsonObject.get("name").toString
       typeValue = typeValue.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
       if("填空题".equals(typeValue)){
         var answerJsonObject =  contentJsonObject.getJSONObject("answer")
         var groupJsonArray = new JSONArray()
         try{
           groupJsonArray = answerJsonObject.getJSONArray("group")
         }catch{
           case e: Exception => {if ("Exception".equals(e)) {
             println("有异常")
             new Breaks().break()
           }}
         }
         for(i <- 0 to groupJsonArray.length()-1){
           var groupJsonObject = groupJsonArray.getJSONObject(i)
           var groupType = groupJsonObject.get("type").toString
           var groupValue = groupJsonObject.get("value").toString
           if("formula".equals(groupType) || "image".equals(groupType)){
             answer_img = answer_img + 1
           }
           if("text".equals(groupType)){
             answer_text = answer_text + groupValue.length
             text_answer_value_temp = text_answer_value_temp + groupValue
           }
         }
       }else if("判断题".equals(typeValue)){
         var answerValue = contentJsonObject.get("answer").toString
         answerValue = answerValue.replaceAll("\u00A0","")
         var answerType = contentJsonObject.get("type").toString
         if("text".equals(answerType)){
           answer_text = answer_text + answerValue.length
           text_answer_value_temp = text_answer_value_temp + answerValue
         }
         if("formula".equals(answerType) || "image".equals(answerType)){
           answer_img = answer_img + 1
         }
       }else{
         var answerJsonArray = new JSONArray()
         try{
           answerJsonArray = contentJsonObject.getJSONArray("answer")
         }catch{
           case e: Exception => {
             var answerValue = contentJsonObject.get("answer").toString.trim
             answerValue = answerValue.replaceAll("\u00A0","")
             answerValue = answerValue.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
             if(!answerValue.contains("http:")){
               if("答案见解析".equals(answerValue)){
                 answer_text = answer_text + answerValue.length
               }
             }
             if(answerValue.contains("http")){
               answer_img = answer_img + 1

             }
           }
         }
         //
         var answerJsonObject = contentJsonObject.getJSONObject("answer")
         var answerType = answerJsonObject.get("type").toString.trim
         var answerValue = answerJsonObject.get("value").toString.trim
         if("formula".equals(answerType) || "image".equals(answerType)){
           answer_img = answer_img + 1
         }
         if("text".equals(answerType)){
           answer_text=answer_text + answerValue.length
           answerValueSum=answerValueSum+answerValue
           text_answer_value_temp=text_answer_value_temp+ answerValue
         }

       }
     }
     text_answer_value_temp = text_answer_value_temp.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
     text_answer_value= text_answer_value+text_answer_value_temp
     if("n".equals(text_answer_value) || "答案略".equals(text_answer_value) || "省略".equals(text_answer_value) || "略".equals(text_answer_value) || "无".equals(text_answer_value) || "暂无".equals(text_answer_value) || text_answer_value==null){
       text_answer_value = "答案为空"
       answer_img = 0
       answer_text=0
     }
     //analysis部分
     var analysisJsonArray = new JSONArray()
     var text_analysis_value_temp =  ""
     try{
       analysisJsonArray = contentJsonObject.getJSONArray("analysis")
       println(analysisJsonArray+"--不顺眼的解析--")
     }catch{
       case e: Exception => {
         var analysisValue = contentJsonObject.get("analysis").toString.trim
         analysisValue=analysisValue.replaceAll("\u00A0","")
         if(!analysisValue.contains("http:")){
           analysis_text=analysis_text+analysisValue.length
           text_analysis_value_temp=text_analysis_value_temp+analysisValue
         }
         if(analysisValue.contains("http:")){
           analysis_img=analysis_img+1
         }
       }
     }
     for(i <- 0 to analysisJsonArray.length()-1){
       try{
         var analysisJsonObject = analysisJsonArray.getJSONObject(i)
         var analysisType = analysisJsonObject.get("type").toString.trim
         var analysisValue  =  analysisJsonObject.get("value").toString.trim
         analysisValue=analysisValue.replaceAll("\u00A0","")
         if("formula".equals(analysisType) || "image".equals(analysisType)){
           answer_img = analysis_img + 1
         }
         if("text".equals(analysisType)){
           analysis_text = analysis_text + analysisValue.length
           text_analysis_value_temp = text_analysis_value_temp + analysisValue
         }

       }catch {
         case e: Exception => {
           new Breaks().break()
         }
       }
       //将解析内容获取到
       text_analysis_value_temp = text_analysis_value_temp.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
       text_analysis_value=text_analysis_value_temp
       if("解析略".equals(text_analysis_value)||"省略".equals(text_analysis_value)||"略".equals(text_analysis_value)||"无".equals(text_analysis_value)||"暂无".equals(text_analysis_value)||"n".equals(text_analysis_value)||text_analysis_value==null){
         text_analysis_value_temp = "解析为空"
         text_analysis_value = text_analysis_value_temp
         analysis_img = 0
         analysis_text = 0
       }
       /*
       * 当题型为证明题、计算题、综合题等大题时，针对一般会出现答案较详细，来需要解析的情况，
			 * 当答案的中出现的图片数（包含公式）大于等于3或文字数大于25时，
			 * 将解析的文字数等于答案的文字数，将解析的图片数等于答案的图片数
       * */
       if(type_id == 6 || type_id == 17 || type_id ==44){
         var analysisSum = analysis_img + analysis_text
         var answerSum = answer_img + answer_text
         if(answerSum > analysisSum){
           analysis_img = answer_img
           analysis_text = answer_text
         }else {
           answer_img = analysis_img
           answer_text = analysis_text
         }
       }
       answerValueSum = answerValueSum.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]","")
       if("答案见解析".equals(answerValueSum) || "见解析".equals(answerValueSum)){
         answer_text = analysis_text
         answer_img = analysis_img
       }
}
  println(text_analysis_value+"-----这是答案和解析----"+text_answer_value)
     body_text +"|"+ body_img + "|" + answer_text + "|" + answer_img + "|" + analysis_text + "|"+ analysis_img + "|" + text_analysis_value + "|" + text_answer_value
     //body_text +","+ body_img + "," + answer_text + "," + answer_img + "," + analysis_text + ","+ analysis_img + "," + text_analysis_value + "," + text_answer_value
   }
}
