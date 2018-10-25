package com.loki.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/*****
 *    从PB对象反向生成proto格式定义， 基本数据格式有小向大兼容
 * @auther Hades
 * @datetime 2018/10/25 15:45
 *
 */

public class ProtobufReflector {
    private static Logger logger = Logger.getLogger(ProtobufReflector.class);

    // 基本数据类型 & 名称映射关系
    private static List<String> stypes = Arrays.asList("float","double","int","long","boolean","Object","ByteString","Float","Double","Integer","Long","Boolean","LazyStringList");
    private static Map<String, String> reflectTypes = new HashMap<String,String>(){
        {
            put("float", "float");
            put("Float", "float");
            put("double", "double");
            put("Double", "Double");
            put("int", "int64");
            put("Integer", "int64");
            put("long", "int64");
            put("Long", "int64");
            put("boolean", "bool");
            put("Boolean", "bool");
            put("Object", "string");      // ???
            put("ByteString", "bytes");
        }
    };

    // 待处理队列
    private Queue<String> q_MsgName = new ConcurrentLinkedQueue<>();
    // 已完成列表
    private Set<String> finishedSet = new HashSet<>();
    // 结果行
    private List<String> lineList = new ArrayList<>();

    /*****
     *  反向解析 PB对象的定义式
     *  	唯一方法入口
     * @param Clazz
     * @return
     * @throws Exception
     */
    public String reflect(Class<?> Clazz) throws Exception{
        // 清理数据
        q_MsgName.clear();
        finishedSet.clear();
        lineList.clear();

        // 入口类 放入队列
        String className = Clazz.getName();
        q_MsgName.add(className);

        // 遍历队列，防止重复处理，不使用递归
        while(q_MsgName.size() > 0){
            className = q_MsgName.poll();
            Class<?> clazz = Class.forName(className);
            reflactClass(clazz);
        }

        String res = StringUtils.join(lineList, "\n");
        // 清理数据
        q_MsgName.clear();
        finishedSet.clear();
        lineList.clear();

        return res;
    }

    /*****
     *  解析类， 遇到Message放进队列
     * @param clazz
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws ClassNotFoundException
     */
    private void reflactClass(Class<?> clazz) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException{
        String cName = clazz.getName();
        // 跳过已完成类
        if(finishedSet.contains(cName)){
            return;
        }

        if(clazz.isEnum()){
            // enum
            lineList.add(String.format("enum %s {", clazz.getSimpleName()));
            Object[] obj = clazz.getEnumConstants();
            Field[] fields = clazz.getFields();

            // reflect getNumber
            Method getNumber = clazz.getMethod("getNumber");
            for(int i = 0; i < fields.length/2; i++){
                // [0-fields.length/2) 为 value_name
                lineList.add(String.format("  %s = %d;", fields[i].getName(), getNumber.invoke(obj[i])));
            }
            lineList.add(String.format("}\n", clazz.getSimpleName()));
            finishedSet.add(cName);
        }
        else {
            // Message
            lineList.add(String.format("message %s {", clazz.getSimpleName()));
            Field[] fields = clazz.getDeclaredFields();
            int i = 0;
            // 不同pb 初始有效字段顺序不一致，找到第一个可以反射成对象的字段作为初始字段
            for (; i < fields.length; i++) {
                Field field = fields[i];
                Object obj = invokeObject(clazz);
                try {
                    field.getInt(obj);
                } catch (Exception e) {
                    continue;
                }
                break;
            }
            // 屏蔽尾部无效字段，有效数据以  x_NUMBER和x相间出现
            for (; i < fields.length - 3; i++) {
                Field field = fields[i++];
                Object obj = invokeObject(clazz);
                int idx = field.getInt(obj);    // 字段序号

                field = fields[i];
                String fieldName = field.getName();
                String typeName = field.getType().getSimpleName();


                switch(typeName){
                    case "LazyStringList":{
                        // repeated string 单独处理
                        lineList.add(String.format("  repeated string %s = %s;", fieldName.replace("_", ""), idx));
                        break;
                    }
                    case "List":{
                        // repeated other类型
                        String AtypeName = field.getAnnotatedType().getType().getTypeName();
                        String className = AtypeName.substring(15, AtypeName.length() - 1);    //  java.lang.List<******> 去掉两边得到类名
                        Class<?> claxx = Class.forName(className);
                        String sname = claxx.getSimpleName();
                        if (!stypes.contains(sname)) {
                            // message 放回队列, 对应行的数据类型不变（message名称）
                            q_MsgName.add(className);
                        } else {
                            // 普通类型，转为 proto 数据类型
                            sname = reflectTypes.get(sname);
                        }
                        lineList.add(String.format("  %s %s %s = %s;", "repeated", sname, fieldName.replace("_", ""), idx));
                        break;
                    }
                    default:{
                        if (stypes.contains(typeName)) {
                            // optional 基本类型， 转为 proto 数据类型
                            typeName = reflectTypes.get(typeName);
                        } else {
                            // optional message 格式 放回队列， 对应行的数据类型不变（message名称）
                            String className = field.getType().getName();
                            q_MsgName.add(className);
                        }
                        lineList.add(String.format("  %s %s %s = %s;", "optional", typeName, fieldName.replace("_", ""), idx));
                    }
                }
            }
            lineList.add("}\n");
            finishedSet.add(cName);
        }
    }

    /*****
     *  反射构建对象
     * @param clazz
     * @return
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    private Object invokeObject(Class<?> clazz) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Method method = clazz.getMethod("newBuilder");
        Object obj = method.invoke(null, new Object[]{});
        return obj;
    }
}
