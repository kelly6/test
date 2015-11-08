#!/usr/bin/python
#coding=utf-8

import RPi.GPIO as GPIO
import time

pin = 5

def init_out():
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(pin, GPIO.OUT)

def init_in():
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)

def on():
    GPIO.output(pin, GPIO.HIGH)

def off():
    GPIO.output(pin, GPIO.LOW)

def callback1(pin):
    print "callback1 pin:", pin

def callback2(pin):
    print "callback2 pin:", pin

if 0:
    init_in()
    inv = GPIO.input(pin)
    print inv
    print dir(GPIO)
    #bouncetime 防抖200ms
    #上升沿 RISING
    #下降沿 FALLING
    #两者都有 BOTH
    #GPIO.wait_for_edge(pin, GPIO.FALLING)
    #------detect------
    #GPIO.add_event_detect(pin, GPIO.FALLING, bouncetime=200)
    #time.sleep(1)
    #print GPIO.event_detected(pin)
    GPIO.add_event_detect(pin, GPIO.RISING, bouncetime=200)
    GPIO.add_event_callback(pin, callback1)
    GPIO.add_event_callback(pin, callback2)
    raw_input(">>")
    GPIO.remove_event_detect(pin)
    raw_input("##")
    GPIO.cleanup()

if 0:
    #out test
    init_out()
    #raw_input(">>")
    #off()
    #on()
    #raw_input(">>")
    #GPIO.cleanup()
    #exit()
    try:
        while 1:
            on()
            time.sleep(10)
            off()
            time.sleep(1)
    except:
        print "got error"
    print "cleaning up..."
    GPIO.cleanup()

if 1:
    #PWM test
    init_out()
    p = GPIO.PWM(pin, 1000)
    p.start(0)
    try:
        while 1:
            for dc in range(0, 101, 5):
                p.ChangeDutyCycle(dc)
                time.sleep(0.1)
            for dc in range(100, -1, -5):
                p.ChangeDutyCycle(dc)
                time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    p.stop()
    GPIO.cleanup()
