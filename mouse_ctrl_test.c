#include <stdio.h>
#include <linux/input.h>
#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

//按鍵模擬，按鍵包含按下和鬆開兩個環節
void simulate_key(int fd, int kval)
{
    struct input_event event;
    gettimeofday(&event.time, 0);
    //按下kval鍵
    event.type = EV_KEY;
    event.value = 1;
    event.code = kval;
    write(fd, &event, sizeof(event));
    //同步，也就是把它報告給系統
    event.type = EV_SYN;
    event.value = 0;
    event.code = SYN_REPORT;
    write(fd, &event, sizeof(event));

    memset(&event, 0, sizeof(event));
    gettimeofday(&event.time, 0);
    //鬆開kval鍵
    event.type = EV_KEY;
    event.value = 0;
    event.code = kval;
    write(fd, &event, sizeof(event));
    //同步，也就是把它報告給系統
    event.type = EV_SYN;
    event.value = 0;
    event.code = SYN_REPORT;
    write(fd, &event, sizeof(event));
}

//鼠標移動模擬
void simulate_mouse(int fd, int rel_x, int rel_y)
{
    struct input_event event;
    int ret;
    gettimeofday(&event.time, 0);
    //x軸坐標的相對位移
    event.type = EV_REL;
    event.value = rel_x;
    event.code = REL_X;
    ret = write(fd, &event, sizeof(event));
    printf("%s:%d ret:%d\n", __FUNCTION__, __LINE__, ret);
    //y軸坐標的相對位移
    event.type = EV_REL;
    event.value = rel_y;
    event.code = REL_Y;
    ret = write(fd, &event, sizeof(event));
    printf("%s:%d ret:%d\n", __FUNCTION__, __LINE__, ret);
    //同步
    event.type = EV_SYN;
    event.value = 0;
    event.code = SYN_REPORT;
    ret = write(fd, &event, sizeof(event));
    printf("%s:%d ret:%d\n", __FUNCTION__, __LINE__, ret);
}

int main(int argc, char **argv)
{
    int fd_mouse = -1;
    int fd_kbd = -1;
    int i = 0;

    /*
    fd_kbd = open("/dev/input/event2", O_RDWR);
    if(fd_kbd <= 0)
    {
        printf("Can not open keyboard input file\n");
        return -1;
    }
    */

    fd_mouse = open("/dev/input/event3", O_RDWR);
    printf("%s:%d fd_mouse:%d\n", __FUNCTION__, __LINE__, fd_mouse);
    if(fd_mouse <= 0)
    {
        printf("Can not open mouse input file\n");
        return -1;
    }

    for (i = 0; i < 50; i++)
    {
        //simulate_key(fd_mouse, BTN_LEFT);  //模擬按下鼠標左鍵
        //if (i % 3 == 0)
        //      simulate_key(fd_kbd, KEY_A);  //模擬按下鍵盤A鍵
        //模擬鼠標相對上次x和y軸相應移動10個像素
        simulate_mouse(fd_mouse, 10, 10);
        sleep(3);
    }
    close(fd_kbd);
    close(fd_mouse);
}
