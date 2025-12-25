# FBDP Lab4：Spark 编程实验

本项目为《FBDP》课程实验四，基于 **Apache Spark**，使用 **RDD、Spark SQL 以及 MLlib** 完成优惠券使用行为的数据分析与预测任务。

---

## 一、实验环境

- 操作系统：Ubuntu 22.04（VMware 虚拟机）
- Java 版本：Java 8
- Scala 版本：2.13.12
- Spark 版本：3.5.7
- 运行模式：local[*]
- VS Code 远程开发

---

## 二、数据集说明

实验使用阿里天池优惠券数据集（CCF O2O 优惠券使用预测），包括：

- `ccf_online_stage1_train.csv`：线上数据
- `ccf_offline_stage1_train.csv`：线下数据；机器学习的训练数据  
- `ccf_offline_stage1_test_revised.csv`：机器学习的测试数据  

---

## 三、项目结构说明

```text
lab4/
├── src/main/scala/
│   ├── Task1_1RDD.scala      # 任务1-1：优惠券整体使用情况统计（RDD）
│   ├── Task1_2RDD.scala      # 任务1-2：指定商家优惠券使用情况（RDD）
│   ├── Task2_1SQL.scala      # 任务2-1：优惠券领取时间分布（Spark SQL）
│   ├── Task2_2SQL.scala      # 任务2-2：商家正样本率统计（Spark SQL）
│   └── Task3ML.scala         # 任务3：优惠券使用预测（MLlib）
│
├── outputs/
│   ├── task1/
│   ├── task2/
│   └── task3/
│
├── data/    （此部分数据由于比较大, 未上传至github）
│   ├── ccf_offline_stage1_test_revised.csv
│   ├── ccf_offline_stage1_train.csv
│   └── ccf_online_stage1_train.csv
│
├── .gitignore
└── README.md

```

## 四、运行方式

```shell
1. 启动 Spark Shell
spark-shell

2. 运行 Scala 程序（以Task3ML.scala为例）
:load src/main/scala/Task3ML.scala
Task3ML.main(Array())
```

## 五、实验结果
实验结果的相关输出文件、控制台的输出截图，均在实验报告中进行了展示。

## 六、实验收获

通过本次实验，掌握了以下内容：
- Spark RDD 与 Spark SQL 的基本编程模式
- 基于 DataFrame 的数据清洗与特征工程方法
- 使用 Spark MLlib 构建机器学习模型的完整流程
- 从数据分析到预测建模的 Spark 项目实践经验

