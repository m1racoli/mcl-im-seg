s = 50;
t = 55;
f = t-s;

M = cl_spat(C,[w h f]);
M = permute(M,[2 1 3]);

for i = 1:f
	subplot(3,f,i);
	CL = cl_viz(-S.ss0(s+i).Z,M(:,:,i));
	imshow(CL,[-2.5 0]);
	title(sprintf('frame %d',s+i));
	subplot(3,f,i + f);
	imshow(-S.ss0(s+i).Z,[-2.5 0]);
	subplot(3,f,i+ 2*f);
	imshow(S.ss0(s+i).intenSR,[0 12000]);
end
