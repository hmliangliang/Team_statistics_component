package com.dm.activity

import com.dm.utils.{SparkConfiguration,Client}
import org.apache.spark.mllib.util.tencent.OptionParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, countDistinct, rand}

object Team_statistics_component{
    def main(args: Array[String]): Unit = {
        // 初始化spark环境
        val options = new OptionParser(args)
        val spark = SparkConfiguration.getSparkSession()
        val client = new Client()
        //配置输入的参数
        val openid = options.getString("openid", "")
        val roleid = options.getString("roleid", "")
        val isjoin = options.getString("isjoin", "") //是否成功加入俱乐部 -1:表示该玩家无点击行为  0:表示该玩家虽然点击，但没有成功加入 1:有点击行为且成功加入
        val clubid = options.getString("clubid", "") //给玩家曝光的俱乐部id
        val algotype = options.getString("algotype", "") // 算法id
        import spark.implicits._
        // 输入的信息表格
        val Array(userDatabase, userTable) =options.getString("data_input").split(",")(0).split("::")
        var table:DataFrame = client.tdw(userDatabase)
          .table(userTable)
          .map(r=>(r(0),r(1),r(2),r(3),r(4)))
          .toDF(Seq(openid,roleid,clubid,isjoin,algotype): _*)
        val exposurenum = table
          .groupBy(algotype)
          .count().withColumnRenamed("count","exposurenum")  //曝光次数
        val exposurepersonnum = table
          .groupBy(algotype)
          .agg(countDistinct(roleid)).withColumnRenamed("count(DISTINCT roleid)","exposurepersonnum") //曝光人数
        val clicknum = table
          .filter(col(isjoin) >= 0)
          .groupBy(algotype)
          .count().withColumnRenamed("count","clicknum") //点击次数
        val clickpersonnum = table
          .filter(col(isjoin) >= 0)
          .groupBy(algotype)
          .agg(countDistinct(roleid)).withColumnRenamed("count(DISTINCT roleid)", "clickpersonnum") //点击人数
        val successnum = table
          .filter(col(isjoin) === 1)
          .groupBy(algotype)
          .count().withColumnRenamed("count","successnum") //成功加入次数
        val successpersonnum = table
          .filter(col(isjoin) === 1)
          .groupBy(algotype)
          .agg(countDistinct(roleid)).withColumnRenamed("count(DISTINCT roleid)", "successpersonnum") //成功加入人数

        println(exposurenum.show())
        println(exposurepersonnum.show())

        //将各个表格拼接起来
        var resultDf = exposurenum
          .join(exposurepersonnum, exposurenum(algotype)===exposurepersonnum(algotype), "left_outer").drop(exposurepersonnum(algotype))
        resultDf = resultDf.join(clicknum, resultDf(algotype)===clicknum(algotype), "left_outer").drop(clicknum(algotype))
        resultDf = resultDf.join(clickpersonnum, resultDf(algotype)===clickpersonnum(algotype), "left_outer").drop(clickpersonnum(algotype))
        resultDf = resultDf.join(successnum, resultDf(algotype)===successnum(algotype), "left_outer").drop(successnum(algotype))
        resultDf = resultDf.join(successpersonnum, resultDf(algotype)===successpersonnum(algotype), "left_outer").drop(successpersonnum(algotype))
        resultDf = resultDf.withColumn("clickrate_num", resultDf("clicknum") / resultDf("exposurenum")) //计算点击率(pair-wise)
        resultDf = resultDf.withColumn(
            "clickrate_person",
            resultDf("clickpersonnum") / resultDf("exposurepersonnum")) //计算点击率(用户)
        resultDf = resultDf.withColumn(
            "exposuresuccessrate_num",
            resultDf("successnum") / resultDf("exposurenum")) //计算曝光成功率(pair-wise)
        resultDf = resultDf.withColumn(
            "exposuresuccessrate_person",
            resultDf("successpersonnum") / resultDf("exposurepersonnum")) //计算曝光成功率(用户)
        resultDf = resultDf.withColumn(
            "clickpassrate_num",
            resultDf("successnum") / resultDf("clicknum")) //计算点击通过率(pair-wise)
        resultDf = resultDf.withColumn(
            "clickpassrate_person",
            resultDf("successpersonnum") / resultDf("clickpersonnum")) //计算点击通过率(用户)

        println(resultDf.show())
        // 输出结果,将表格的数据写入tdw表中
        val Array(resultdb, resulttable) =
            options.getString("data_output").split(",")(0).split("::")
        client.writeDataFrameToTable(
            resultdb,
            resulttable,
            resultDf,
            partValue = "",
            replace = false
        )
    }
}