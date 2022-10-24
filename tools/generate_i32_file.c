#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>		/* exit */
#include <stdio.h>		/* printf */

int		urandom_fd = -2;

void
urandom_init()
{
	urandom_fd = open("/dev/urandom", O_RDONLY);

	if (urandom_fd == -1) {
		int		errsv = urandom_fd;
		printf("Error opening [/dev/urandom]: %i\n", errsv);
		exit(1);
	}
}

unsigned int
urandom()
{
	unsigned int	buf_impl;
	unsigned int   *buf = &buf_impl;

	if (urandom_fd == -2) {
		urandom_init();
	}
	/* Read 4 bytes, or 32 bits into *buf, which points to buf_impl */
	read(urandom_fd, buf, sizeof(int));
	return buf_impl;
}

int
main()
{
	int		r         , rc, vcnt = 0;
	const int	numbers_of_integer = 507229825;
	FILE           *ifp;
	if ((ifp = fopen("huge_int_set", "w+")) == NULL) {
		printf("Failed to open file!\n");
		return 1;
	}
	while (vcnt < numbers_of_integer) {
		r = urandom();
	        vcnt++;
		rc = fprintf(ifp, "%d\n", r);
		if (rc < 0) {
			printf("errno=%d, err_msg=\"%s\"\n", errno, strerror(errno));
			return 1;
		}
	}
	fclose(ifp);
	return 0;
}

