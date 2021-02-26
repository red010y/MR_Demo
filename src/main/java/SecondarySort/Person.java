package SecondarySort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//序列化，比较
public class Person implements WritableComparable<Person> {
    private String name;
    private int age;
    private int salary;

    public Person() {
    }

    public Person(String name, int age, int salary) {
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", salary=" + salary +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    //作为map的输出参数,默认就会调用compareTo进行比较
    //先比较salary降序排列；若相同，按照age升序排列
    public int compareTo(Person o) {
        //计算的值和排序有什么关系？？？
        // 还是不懂，为什么返回一个-的值，这个方法返回的是一个int，怎么就排序了？
        // 比较大小的规则，根据返回的数值正负进行排序，默认升序。这里加个-号正好反过来
        int compareResult = this.salary - o.salary;
        if (compareResult!=0){
            //默认是小的在前（升序排序,相减不过是确定下大小关系），这里来个-正好弄成大的在前
            return -compareResult;
        }else {
            //相减就是确定下大小关系,之后按照默认排序进行排序
            return this.age-o.age;
        }
    }

    //map的写，reduce的读写。就调用这两个读写的方法
    @Override
    //序列化，将Key转化成使用流传送的二进制
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(salary);
    }

    @Override
    //读字段的顺序，要与write方法中写的顺序保持一致
    public void readFields(DataInput in) throws IOException {
        this.name=in.readUTF();//string
        this.age=in.readInt();
        this.salary=in.readInt();
    }
}
