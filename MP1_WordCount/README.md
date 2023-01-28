# MP1 Word Count
A Machine Programming Assignment for CS498: Cloud Computing Applications UIUC Spring 2023 MP1.

## Requirement
Edit the java template file: MP1.java. All you need to edit is the part marked with TODO.
<br>
Compile the file and run it on the provided input, read from the standard input (STDIN). Your program should utilize an input argument number as a seed. The following is an example of a command to run the application, using ‘1’ as the argument. 
```shell script
javac MP1.java
cat input.txt | java MP1 1
```
Do not change the filename, class name, or the main() function.

## Steps
1. Divide each sentence into a list of words using delimiters provided in the “delimiters” variable.
2. Make all the tokens lowercase and remove any trailing and leading spaces.
3. Ignore all common words provided in the “stopWordsArray” variable.
4. Keep track of word frequencies. To make the application more interesting, you must process only the titles with certain indexes. These indexes are accessible using the “getIndexes” method, which returns an integer array with 0-based indexes to the input file. It is possible to have an index appear several times. In this case, process the index multiple times.
5. Sort the words by frequency in descending order. If two words have the same number count, use lexigraphy. For example, the following is sorted.
6. Print out the top 20 items from the sorted list as a String Array.
