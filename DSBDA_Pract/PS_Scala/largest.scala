object largest{
    def main(args: Array[String]): Unit = {
        var num1: Int = 0;
        var num2: Int = 0;

        print("Enter number1: ")
        num1 = scala.io.StdIn.readInt()
        print("Enter number1: ")
        num2 = scala.io.StdIn.readInt()

        if(num1 > num2){
            println(s"$num1 is greater than $num2")
        }
        else if(num1 < num2){
            println(s"$num2 is greater than $num1")

        }
    }
}