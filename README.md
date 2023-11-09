# Word-Count-and-Phrase-Analysis-of-Swahili-Language-Using-Spark-RDD

<p> In this project, our objective is to develop a word count function that performs phrase analysis using Spark RDD (Resilient Distributed Datasets). The main focus lies within the function "run_phrase_count_on_rdd()" where several transformations are applied to the RDD to generate phrase counts. The key steps involved in this process include filtering the RDD to skip header and bad rows, utilizing map operations to split each line into columns and convert text into n-grams using NLTK functions, employing flatMap to flatten the RDD for subsequent reduction, and finally, choosing between reduce or reduceByKey operations. Detailed information on these steps can be found in the Spark RDD API documentation and the "introducing-spark-APIs" notebook. </p>

<p>Before executing the function, it is recommended to test the code for computing phrase counts independently. It is advisable to compute one transformation at a time and verify its correctness before chaining multiple operations together. Additionally, ensure that the function "tokenize_into_ngrams_phrase(text, n=3)" is functioning correctly by testing it before using it in the map operation.</p>

<p>To explore the data and perform testing, loading the data into a pandas DataFrame is encouraged. The expected number of columns in the output CSV file is 3, namely phrase, count, and phrase_en, which will be checked during the grading process.</p>

## Converting Texts into N-grams

<p>For generating n-grams effortlessly, we recommend using the NLTK module and specifically the "ngrams" function. Additionally, the "sent_tokenize" function from the "nltk.tokenize" module will be useful. Exploring the NLTK documentation will provide a better understanding of how to use these functions effectively.</p>

## Cleaning up Phrases

<p>During tokenization, nonsensical and noisy phrases such as 0000 or blank strings may be generated. It is crucial to remove these from the results. You have the flexibility to use any approach for this task. However, a function called "quick_clean_up()" has been provided, which you can use or complete according to your preference, utilizing the phrase length to perform the cleanup.</p>

## Translating to English

<p> The phrases in the dataset are in Swahili, but the output CSV file will contain translated English phrases. Google Translate is suggested for the translation process, but you can opt for an alternative translation package if desired. Due to the relatively slow translation speed, it is recommended to translate only the top-k occurring phrases (after cleanup). For example, translating 500 phrases may take around 2-3 minutes. Adjusting the value of k to a small number during testing is advised. Familiarize yourself with the usage of the translation package, whether Google Translate or another alternative, by referring to its documentation page.</p>

<p>Through this project, we aim to develop a robust word count function that incorporates phrase analysis using Spark RDD. By following the provided steps, addressing phrase cleanup, and performing translation to English, we can gain valuable insights from the Swahili text data. The project will provide clear instructions, well-documented code, and guidance on utilizing Spark RDD and NLTK functions. Our objective is to empower users to effectively perform word count and phrase analysis tasks, unlocking the potential of NLP techniques for Swahili language data.</p>
